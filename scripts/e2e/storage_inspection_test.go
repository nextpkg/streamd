package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInspectionBarrierDoesNotMaskClientCancellation(test *testing.T) {
	var forwarded atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded.Add(1) }))
	defer target.Close()
	proxy, err := newInspectionProxy(target.URL, make(chan struct{}))
	require.NoError(test, err)
	defer proxy.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxy.server.URL+"/?uploads&prefix=sample/", nil)
	require.NoError(test, err)
	done := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if response != nil {
			response.Body.Close()
		}
		done <- err
	}()
	require.Eventually(test, func() bool {
		proxy.mu.Lock()
		defer proxy.mu.Unlock()
		return proxy.directories["sample/"] == 1
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.True(test, errors.Is(err, context.Canceled), "request error: %v", err)
	case <-time.After(time.Second):
		test.Fatal("cancelled request did not exit")
	}
	require.Eventually(test, func() bool {
		proxy.mu.Lock()
		defer proxy.mu.Unlock()
		return proxy.windows["sample/"].Cancelled
	}, time.Second, time.Millisecond)
	require.Zero(test, forwarded.Load())
}
