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

func TestFinalizationFaultRejectsUnrelatedServerErrors(t *testing.T) {
	message := "close failed,sync=true,block=true: operation error S3: CompleteMultipartUpload: net/http: timeout awaiting response headers"
	var log strings.Builder
	encoder := json.NewEncoder(&log)
	require.NoError(t, encoder.Encode(map[string]string{"level": "ERROR", "msg": message}))
	require.NoError(t, encoder.Encode(map[string]string{"level": "ERROR", "msg": "[flv/sample]recording configuration finalization: " + message}))
	require.NoError(t, encoder.Encode(map[string]string{"level": "INFO", "msg": "shutdown"}))
	matched, err := finalizationErrors([]byte(log.String()))
	require.NoError(t, err)
	require.Len(t, matched, 2)
	require.NoError(t, encoder.Encode(map[string]string{"level": "ERROR", "msg": "unrelated media corruption"}))
	_, err = finalizationErrors([]byte(log.String()))
	require.ErrorContains(t, err, "unrelated media corruption")
	_, err = finalizationErrors(nil)
	require.Error(t, err)
	_, err = finalizationErrors([]byte("panic: unexpected failure\n"))
	require.Error(t, err)
}

func TestCompletionFaultOnlyStallsMultipartCompletion(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		response.WriteHeader(http.StatusCreated)
	}))
	defer backend.Close()
	fault, err := newCompletionFault(backend.URL)
	require.NoError(t, err)
	defer fault.close()
	fault.enabled.Store(true)
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	for _, probe := range []struct{ method, query string }{{"GET", "?uploadId=test"}, {"PUT", "?uploadId=test&partNumber=1"}, {"POST", "?uploads"}} {
		request, err := http.NewRequest(probe.method, fault.server.URL+probe.query, strings.NewReader("payload"))
		require.NoError(t, err)
		response, err := client.Do(request)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode)
		require.NoError(t, response.Body.Close())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "POST", fault.server.URL+"?uploadId=test", strings.NewReader("completion"))
	require.NoError(t, err)
	_, err = client.Do(request)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Eventually(t, func() bool { return fault.cancelled.Load() == 1 }, time.Second, time.Millisecond)
	require.Equal(t, int64(1), fault.requests.Load())
}
