package httpx

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type callbackTransportFunc func(*http.Request) (*http.Response, error)

func (transport callbackTransportFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func callbackSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("callback synchronization deadline exceeded")
	}
}

func TestCallbackResponseContract(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		status int
		body   string
		valid  bool
	}{
		{"empty", http.StatusOK, "", true},
		{"exact-limit", http.StatusOK, strings.Repeat("x", callbackResponseLimit), true},
		{"over-limit", http.StatusOK, strings.Repeat("x", callbackResponseLimit+1), false},
		{"no-content-not-original-success", http.StatusNoContent, "", false},
		{"server-error", http.StatusServiceUnavailable, "private-response", false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			requestErrors := make(chan string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				body, err := io.ReadAll(request.Body)
				if err != nil || request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" || string(body) != `{"event":"test"}` || request.URL.RawQuery != "keep=one&keep=two" {
					requestErrors <- "request contract changed"
				}
				response.WriteHeader(scenario.status)
				_, _ = io.WriteString(response, scenario.body)
			}))
			defer server.Close()
			executor := newCallbackExecutor()
			defer executor.close()
			if executor.client.Timeout != 3*time.Second || executor.client.Transport == http.DefaultTransport {
				t.Fatal("callbacks do not own the production three-second transport")
			}
			err := executor.post(server.URL+"?keep=one&keep=two", []byte(`{"event":"test"}`))
			if (err == nil) != scenario.valid || (err != nil && strings.Contains(err.Error(), "private-response")) {
				t.Fatalf("unexpected callback result: %v", err)
			}
			select {
			case failure := <-requestErrors:
				t.Fatal(failure)
			default:
			}
		})
	}
}

func TestCallbackTimeoutCancelsHeadersAndBody(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			exited := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				_, _ = io.Copy(io.Discard, request.Body)
				if phase == "body" {
					response.WriteHeader(http.StatusOK)
					response.(http.Flusher).Flush()
				}
				<-request.Context().Done()
				close(exited)
			}))
			defer server.Close()
			executor := newCallbackExecutor()
			defer executor.close()
			executor.client.Timeout = 150 * time.Millisecond
			started := time.Now()
			err := executor.post(server.URL, []byte(`{}`))
			if err == nil || !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
				t.Fatalf("callback %s did not fail within its budget: %v", phase, err)
			}
			callbackSignal(t, exited)
		})
	}
}

func TestCallbackCapacityAndConcurrentClose(t *testing.T) {
	entered, exited := make(chan struct{}, callbackCapacity), make(chan struct{}, callbackCapacity)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		entered <- struct{}{}
		<-request.Context().Done()
		exited <- struct{}{}
	}))
	defer server.Close()
	executor := newCallbackExecutor()
	defer executor.close()
	failures := make(chan error, callbackCapacity)
	for range callbackCapacity {
		if err := executor.submit(server.URL, []byte(`{}`), func(err error) { failures <- err }); err != nil {
			t.Fatal(err)
		}
	}
	for range callbackCapacity {
		callbackSignal(t, entered)
	}
	started := time.Now()
	if err := executor.submit(server.URL, []byte(`{}`), nil); err == nil || time.Since(started) > 100*time.Millisecond {
		t.Fatal("saturated callbacks queued or blocked another caller")
	}
	var closers sync.WaitGroup
	for range 16 {
		closers.Go(executor.close)
	}
	closed := make(chan struct{})
	go func() { closers.Wait(); close(closed) }()
	callbackSignal(t, closed)
	if time.Since(started) >= 2*time.Second {
		t.Fatal("shutdown waited for the ordinary request timeout rather than cancelling")
	}
	for range callbackCapacity {
		callbackSignal(t, exited)
		select {
		case err := <-failures:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected cancellation result: %v", err)
			}
		default:
			t.Fatal("close returned before callback error reporting retired")
		}
	}
	if executor.active != 0 || executor.submit(server.URL, nil, nil) == nil {
		t.Fatal("closed executor retained or admitted callback work")
	}
}

func TestCallbackAdmissionRacesWithClose(t *testing.T) {
	for range 20 {
		executor := newCallbackExecutor()
		var accepted, completed atomic.Int64
		executor.client.Transport = callbackTransportFunc(func(request *http.Request) (*http.Response, error) {
			<-request.Context().Done()
			return nil, request.Context().Err()
		})
		start := make(chan struct{})
		var workers sync.WaitGroup
		for range 80 {
			workers.Go(func() {
				<-start
				if executor.submit("http://callback.invalid/", []byte(`{}`), func(error) { completed.Add(1) }) == nil {
					accepted.Add(1)
				}
			})
		}
		workers.Go(func() { <-start; executor.close() })
		close(start)
		workers.Wait()
		executor.close()
		if accepted.Load() != completed.Load() || executor.active != 0 {
			t.Fatalf("work escaped close: accepted=%d completed=%d active=%d", accepted.Load(), completed.Load(), executor.active)
		}
	}
}

func TestCallbackOwnsSubmittedPayload(t *testing.T) {
	executor := newCallbackExecutor()
	defer executor.close()
	release := make(chan struct{})
	observed := make(chan string, 1)
	executor.client.Transport = callbackTransportFunc(func(request *http.Request) (*http.Response, error) {
		<-release
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		observed <- string(body)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})
	body := []byte(`{"event":"original"}`)
	if err := executor.submit("http://callback.invalid/", body, nil); err != nil {
		t.Fatal(err)
	}
	copy(body, strings.Repeat("x", len(body)))
	close(release)
	select {
	case actual := <-observed:
		if actual != `{"event":"original"}` {
			t.Fatal("caller mutation changed the admitted event")
		}
	case <-time.After(time.Second):
		t.Fatal("callback did not execute")
	}
}
