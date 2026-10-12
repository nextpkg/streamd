package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUploadFaultModesAndRequestScope(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		response.WriteHeader(http.StatusCreated)
	}))
	defer backend.Close()
	for _, mode := range []string{"queue", "timeout", "reset", "quota"} {
		t.Run(mode, func(t *testing.T) {
			fault, err := newUploadFault(backend.URL, mode)
			require.NoError(t, err)
			defer fault.close()
			client := &http.Client{Timeout: time.Second}
			defer client.CloseIdleConnections()
			fault.enabled.Store(true)
			for _, probe := range []struct{ method, query string }{{"GET", "?uploadId=test&partNumber=1"}, {"PUT", "?uploadId=test"}, {"POST", "?uploadId=test"}, {"DELETE", "?uploadId=test"}} {
				request, err := http.NewRequest(probe.method, fault.server.URL+probe.query, strings.NewReader("probe"))
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusCreated, response.StatusCode)
				require.NoError(t, response.Body.Close())
			}
			ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, "PUT", fault.server.URL+"?uploadId=test&partNumber=1", strings.NewReader("media"))
			require.NoError(t, err)
			response, err := client.Do(request)
			switch mode {
			case "queue", "timeout":
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Eventually(t, func() bool { return fault.cancelled.Load() == 1 }, time.Second, time.Millisecond)
			case "reset":
				require.Error(t, err)
				require.Eventually(t, func() bool { return fault.resets.Load() == 1 }, time.Second, time.Millisecond)
			case "quota":
				require.NoError(t, err)
				require.Equal(t, http.StatusInsufficientStorage, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.Contains(t, string(body), "<Code>InsufficientStorage</Code>")
				require.NoError(t, response.Body.Close())
			}
			require.Equal(t, int64(1), fault.requests.Load())
		})
	}
	_, err := newUploadFault(backend.URL, "unknown")
	require.Error(t, err)
}

func TestStorageFaultLogsRequireExpectedCauseAndRecorderReport(t *testing.T) {
	samples := map[string]string{
		"queue":   "recording upload queue capacity exceeded",
		"timeout": "operation error S3: UploadPart: timeout awaiting response headers",
		"reset":   "operation error S3: UploadPart: connection reset by peer",
		"quota":   "operation error S3: UploadPart: InsufficientStorage: injected capacity exhaustion",
	}
	for mode, message := range samples {
		var log strings.Builder
		encoder := json.NewEncoder(&log)
		require.NoError(t, encoder.Encode(map[string]string{"level": "ERROR", "msg": message}))
		match := func(candidate string) bool { return uploadErrorMatches(mode, candidate) }
		_, err := faultErrors([]byte(log.String()), match)
		require.ErrorContains(t, err, "missing recording failure")
		require.NoError(t, encoder.Encode(map[string]string{"level": "ERROR", "msg": "[flv/sample]recording failed: " + message}))
		expected, err := faultErrors([]byte(log.String()), match)
		require.NoError(t, err)
		require.Len(t, expected, 2)
		require.NoError(t, encoder.Encode(map[string]string{"level": "ERROR", "msg": "unrelated media corruption"}))
		_, err = faultErrors([]byte(log.String()), match)
		require.ErrorContains(t, err, "unrelated media corruption")
	}
	require.False(t, uploadErrorMatches("reset", "subscriber EOF"))
	require.False(t, uploadErrorMatches("quota", "operation error S3: UploadPart: AccessDenied"))
	require.False(t, uploadErrorMatches("timeout", samples["queue"]))
}
