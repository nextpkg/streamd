package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadQueryCLIPrintsValuesAndRetainsMachineReadableEvidence(test *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		fmt.Fprint(response, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"sample"},"value":[1,"12.5"]}]}}`)
	}))
	defer server.Close()
	var output bytes.Buffer
	err := execute(context.Background(), []string{"query", "-address", server.URL, "-out", test.TempDir()}, &output, io.Discard)
	require.NoError(test, err)
	require.Contains(test, output.String(), "cpu_percent\t")
	require.Contains(test, output.String(), "resident_memory_mib\t")
	require.Contains(test, output.String(), "\t12.5\n")
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	body, err := os.ReadFile(filepath.Join(lines[len(lines)-1], "results.json"))
	require.NoError(test, err)
	var result report
	require.NoError(test, json.Unmarshal(body, &result))
	require.True(test, result.Complete)
	require.Len(test, result.Metrics, 2)
}

func TestLoadQueriesEncodeExpressionsAndPreserveEverySeries(test *testing.T) {
	queries := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/prom/api/v1/query" || request.Method != http.MethodGet {
			response.WriteHeader(http.StatusNotFound)
			return
		}
		queries <- request.URL.Query().Get("query")
		fmt.Fprint(response, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"first"},"value":[1,"12.5"]},{"metric":{"instance":"second"},"value":[1,"17.25"]}]}}`)
	}))
	defer server.Close()
	result, err := queryMetrics(context.Background(), options{address: server.URL + "/prom/", job: "streamd"})
	require.NoError(test, err)
	require.Len(test, result, 2)
	require.Len(test, queries, 2)
	require.Contains(test, <-queries, `rate(process_cpu_seconds_total{job="streamd"}[1m])`)
	require.Contains(test, <-queries, "process_resident_memory_bytes")
	require.Len(test, result[0].Samples, 2)
	require.Equal(test, 12.5, result[0].Samples[0].Value)
	require.Equal(test, "second", result[1].Samples[1].Labels["instance"])
	custom := `sum(rate(test_total{label="a+b c"}[1m]))`
	result, err = queryMetrics(context.Background(), options{address: server.URL + "/prom", query: custom})
	require.NoError(test, err)
	require.Len(test, result, 1)
	require.Equal(test, custom, <-queries)
}

func TestLoadQueriesRejectInvalidEmptyAndUnboundedResponses(test *testing.T) {
	for _, body := range []string{
		"not-json", `{"status":"error","error":"denied"}`, `{"status":"success","data":{"resultType":"vector","result":[]}}`,
		`{"status":"success","data":{"resultType":"matrix","result":[]}}`,
		`{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"NaN"]}]}}`,
		`{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"+Inf"]}]}}`,
		`{"status":"success","data":{"resultType":"vector","result":[{"value":[1]}]}}`, strings.Repeat("x", 1024*1024+1),
	} {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { fmt.Fprint(response, body) }))
		_, err := queryInstant(context.Background(), server.Client(), server.URL, "up")
		server.Close()
		require.Error(test, err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { response.WriteHeader(http.StatusForbidden) }))
	defer server.Close()
	_, err := queryInstant(context.Background(), server.Client(), server.URL, "up")
	require.ErrorContains(test, err, "403")
	_, err = queryMetrics(context.Background(), options{job: `injected"label`})
	require.Error(test, err)
}

func TestLoadQueryCancellationReleasesHTTPRequests(test *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		close(started)
		<-request.Context().Done()
		close(cancelled)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := queryInstant(ctx, server.Client(), server.URL, "up"); finished <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		test.Fatal("request did not start")
	}
	cancel()
	require.ErrorIs(test, <-finished, context.Canceled)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		test.Fatal("request was not cancelled")
	}
}
