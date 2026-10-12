package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextpkg/streamd/config"
)

func mediaProxy(test *testing.T, handler http.Handler) *Server {
	test.Helper()
	proxyConfiguration(test, config.Proxy{Prefix: "edge"})
	proxyUpstream(test, handler)
	config.GetFinder().Register("live", "sample")
	router := Init()
	test.Cleanup(router.Close)
	return router
}

func proxyClient(test *testing.T) *http.Client {
	test.Helper()
	transport := &http.Transport{DisableCompression: true}
	test.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}
}

func waitProxySignal(test *testing.T, signal <-chan struct{}, description string) {
	test.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		test.Fatal(description)
	}
}

func waitProxyRetirement(test *testing.T, router *Server) {
	test.Helper()
	limit := time.Now().Add(3 * time.Second)
	for time.Now().Before(limit) {
		router.mutex.Lock()
		remaining := len(router.requests)
		router.mutex.Unlock()
		if remaining == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	test.Fatal("proxy retained active response owners")
}

func TestProxyPreservesMediaResponseAndRequestSemantics(test *testing.T) {
	requests := make(chan *http.Request, 8)
	router := mediaProxy(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests <- request.Clone(context.Background())
		response.Header().Set("Content-Type", "video/mp2t")
		response.Header().Set("Content-Range", "bytes 3-6/10")
		response.Header().Set("Cache-Control", "max-age=4")
		response.Header().Set("ETag", "\"segment\"")
		response.Header().Set("Content-Length", "4")
		response.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(response, "data")
	}))
	server := httptest.NewServer(router)
	defer server.Close()
	client := proxyClient(test)
	for _, route := range []string{"/hfs/live/sample.flv", "/hls/live/sample.m3u8", "/hls/live/sample/1.ts"} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			request, err := http.NewRequest(method, server.URL+"/edge"+route+"?part=a%2Bb&part=next", nil)
			if err != nil {
				test.Fatal(err)
			}
			request.Header.Set("Range", "bytes=3-6")
			response, err := client.Do(request)
			if err != nil {
				test.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			expected := "data"
			if method == http.MethodHead {
				expected = ""
			}
			if err != nil || string(body) != expected || response.StatusCode != http.StatusPartialContent || response.Header.Get("ETag") != "\"segment\"" || response.Header.Get("Content-Range") != "bytes 3-6/10" || response.Header.Get("Cache-Control") != "max-age=4" {
				test.Fatalf("response changed: %s %s %v %v %q", method, route, err, response, body)
			}
			forwarded := <-requests
			if forwarded.Method != method || forwarded.URL.Path != route || forwarded.URL.RawQuery != request.URL.RawQuery || forwarded.Host != request.URL.Host || forwarded.Header.Get("Range") != "bytes=3-6" {
				test.Fatalf("request changed: %+v", forwarded)
			}
		}
	}
	for _, route := range []string{"/else/hfs/live/sample.flv", "/edge/hfs/live/sample.flv"} {
		request := httptest.NewRequest(http.MethodPost, route, nil)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		expected := http.StatusMethodNotAllowed
		if strings.HasPrefix(route, "/else/") {
			expected = http.StatusNotFound
		}
		if response.Code != expected {
			test.Fatalf("unexpected route status: %s %d", route, response.Code)
		}
	}
	if len(requests) != 0 {
		test.Fatal("invalid methods reached upstream")
	}
}

func TestProxySignatureRejectsBeforeUpstreamOrCallback(test *testing.T) {
	var requests atomic.Int64
	upstream := proxyUpstream(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(response, "media")
	}))
	proxyConfiguration(test, config.Proxy{Prefix: "edge", EnableSecure: true, SecureKey: "test-secret", SecureTTL: 60, OnNotFound: upstream.URL})
	config.GetFinder().Register("live", "sample")
	router := Init()
	defer router.Close()
	for _, instance := range []string{"sample", "absent"} {
		for _, offset := range []int64{0, -5, 30, 3600} {
			expiration := time.Now().Unix() + offset
			token := config.AntiLeechSign("live/"+instance, "test-secret", expiration)
			query := fmt.Sprintf("?e=%d&token=%s", expiration, token)
			expected := http.StatusForbidden
			if offset == 0 {
				query = ""
			} else if offset == -5 {
				expected = http.StatusGone
			} else if offset == 30 {
				expected = http.StatusOK
				if instance == "absent" {
					expected = http.StatusNotFound
				}
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/edge/hfs/live/"+instance+".flv"+query, nil))
			if response.Code != expected {
				test.Fatalf("instance=%s offset=%d status=%d expected=%d", instance, offset, response.Code, expected)
			}
		}
	}
	if requests.Load() != 2 {
		test.Fatalf("unsigned or expired requests reached upstream/callback: %d", requests.Load())
	}
}

func TestProxyCloseCancelsAndJoinsLiveUpstreams(test *testing.T) {
	var retired atomic.Int64
	router := mediaProxy(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		defer retired.Add(1)
		_, _ = io.WriteString(response, "live")
		_ = http.NewResponseController(response).Flush()
		<-request.Context().Done()
	}))
	router.HandleFunc("GET /health", func(response http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(response, "ok") })
	server := httptest.NewServer(router)
	defer server.Close()
	client := proxyClient(test)
	var workers sync.WaitGroup
	for index := 0; index < 8; index++ {
		response, err := client.Get(server.URL + "/edge/hfs/live/sample.flv")
		if err != nil {
			test.Fatal(err)
		}
		workers.Add(1)
		go func() { defer workers.Done(); _, _ = io.Copy(io.Discard, response.Body); _ = response.Body.Close() }()
	}
	for index := 0; index < 16; index++ {
		workers.Add(1)
		go func() { defer workers.Done(); router.Close() }()
	}
	joined := make(chan struct{})
	go func() { workers.Wait(); close(joined) }()
	waitProxySignal(test, joined, "concurrent shutdown did not release all requests")
	waitProxyRetirement(test, router)
	deadline := time.Now().Add(time.Second)
	for retired.Load() < 8 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if retired.Load() != 8 {
		test.Fatalf("shutdown left upstreams alive: %d/8 retired", retired.Load())
	}
	response, err := client.Get(server.URL + "/health")
	if err != nil {
		test.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(body) != "ok" {
		test.Fatal("proxy shutdown interfered with unrelated local handler")
	}
}

func TestProxyClientCancellationReleasesPendingHeaders(test *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	router := mediaProxy(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		close(entered)
		<-request.Context().Done()
		close(cancelled)
	}))
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/edge/hfs/live/sample.flv", nil)
	client := proxyClient(test)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		response, err := client.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		if !errors.Is(err, context.Canceled) {
			test.Errorf("cancellation was hidden: %v", err)
		}
	}()
	waitProxySignal(test, entered, "request did not reach upstream")
	cancel()
	waitProxySignal(test, finished, "cancelled client did not return")
	waitProxySignal(test, cancelled, "cancelled client retained upstream")
	waitProxyRetirement(test, router)
}

func TestProxyCloseCancelsMissingSourceCallback(test *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	callback := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		close(entered)
		<-request.Context().Done()
		close(cancelled)
	}))
	defer callback.Close()
	proxyConfiguration(test, config.Proxy{Prefix: "edge", OnNotFound: callback.URL})
	router := Init()
	defer router.Close()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/edge/hfs/live/absent.flv", nil))
	}()
	waitProxySignal(test, entered, "callback did not start")
	closed := make(chan struct{})
	go func() { router.Close(); close(closed) }()
	waitProxySignal(test, closed, "close waited for callback timeout instead of cancellation")
	waitProxySignal(test, cancelled, "callback was not cancelled")
	waitProxySignal(test, finished, "callback request did not finish")
	waitProxyRetirement(test, router)
}

func TestProxyMissingSourceCallbackResponseIsBounded(test *testing.T) {
	body := strings.Repeat("missing", 200000)
	callback := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(response, body) }))
	defer callback.Close()
	proxyConfiguration(test, config.Proxy{Prefix: "edge", OnNotFound: callback.URL})
	router := Init()
	defer router.Close()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/edge/hfs/live/absent.flv", nil))
	if response.Code != http.StatusNotFound || response.Body.Len() != 1024*1024 || response.Body.String() != body[:1024*1024] {
		test.Fatalf("callback limit changed: %d %d", response.Code, response.Body.Len())
	}
}

func TestProxySlowDownstreamRetiresWithoutResumingReads(test *testing.T) {
	upstreamDone := make(chan struct{})
	const total = 64 * 1024 * 1024
	router := mediaProxy(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Has("healthy") {
			_, _ = io.WriteString(response, "healthy")
			return
		}
		defer close(upstreamDone)
		response.Header().Set("Content-Length", strconv.Itoa(total))
		chunk := bytes.Repeat([]byte{0x47}, 32*1024)
		for written := 0; written < total; written += len(chunk) {
			if _, err := response.Write(chunk); err != nil {
				return
			}
		}
	}))
	router.responseTimeout = 100 * time.Millisecond
	closed := make(chan struct{})
	var closeOnce sync.Once
	server := httptest.NewUnstartedServer(router)
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closeOnce.Do(func() { close(closed) })
		}
	}
	server.Start()
	defer server.Close()
	transport, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if err != nil {
		test.Fatal(err)
	}
	defer transport.Close()
	connection := transport.(*net.TCPConn)
	if err := connection.SetReadBuffer(1024); err != nil {
		test.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/edge/hls/live/sample/1.ts", nil)
	if err := request.Write(connection); err != nil {
		test.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), request)
	if err != nil {
		test.Fatal(err)
	}
	waitProxySignal(test, upstreamDone, "stalled downstream kept upstream alive")
	waitProxyRetirement(test, router)
	waitProxySignal(test, closed, "retired response kept downstream connection open")
	if response.ContentLength != total {
		test.Fatal("upstream did not announce the full test body")
	}
	if err := connection.Close(); err != nil {
		test.Fatal(err)
	}
	healthy, err := proxyClient(test).Get(server.URL + "/edge/hfs/live/sample.flv?healthy")
	if err != nil {
		test.Fatal(err)
	}
	body, err := io.ReadAll(healthy.Body)
	_ = healthy.Body.Close()
	if err != nil || string(body) != "healthy" {
		test.Fatalf("stalled request affected independent playback: %s %v", body, err)
	}
}

func TestProxyHealthyStreamingRenewsDeadlinesAndReusesConnection(test *testing.T) {
	router := mediaProxy(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Length", "6")
		for index := 0; index < 6; index++ {
			_, _ = io.WriteString(response, "m")
			_ = http.NewResponseController(response).Flush()
			time.Sleep(20 * time.Millisecond)
		}
	}))
	router.responseTimeout = 75 * time.Millisecond
	server := httptest.NewServer(router)
	defer server.Close()
	connection, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if err != nil {
		test.Fatal(err)
	}
	defer connection.Close()
	reader := bufio.NewReader(connection)
	for index := 0; index < 4; index++ {
		_ = connection.SetDeadline(time.Now().Add(time.Second))
		request, _ := http.NewRequest(http.MethodGet, server.URL+"/edge/hfs/live/sample.flv", nil)
		if err := request.Write(connection); err != nil {
			test.Fatal(err)
		}
		response, err := http.ReadResponse(reader, request)
		if err != nil {
			test.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || string(body) != "mmmmmm" || response.Close {
			test.Fatalf("healthy connection lost: %q %v close=%v", body, err, response.Close)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestProxyTruncatedUpstreamCannotBecomeSuccessfulResponse(test *testing.T) {
	router := mediaProxy(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Length", "100")
		_, _ = io.WriteString(response, "partial")
	}))
	server := httptest.NewServer(router)
	defer server.Close()
	response, err := proxyClient(test).Get(server.URL + "/edge/hls/live/sample/1.ts")
	if err != nil {
		test.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if string(body) != "partial" || !errors.Is(err, io.ErrUnexpectedEOF) {
		test.Fatalf("partial media became successful: %q %v", body, err)
	}
	waitProxyRetirement(test, router)
}

type failedProxyResponse struct {
	*httptest.ResponseRecorder
	deadlineError error
	flushError    error
	short         bool
}

func (response *failedProxyResponse) SetWriteDeadline(time.Time) error { return response.deadlineError }
func (response *failedProxyResponse) FlushError() error                { return response.flushError }
func (response *failedProxyResponse) Write(body []byte) (int, error) {
	if response.short {
		return len(body) - 1, nil
	}
	return response.ResponseRecorder.Write(body)
}

func TestProxyResponsePropagatesWriteAndFlushFailures(test *testing.T) {
	for _, failure := range []string{"deadline", "short", "flush"} {
		router := Init()
		response := &failedProxyResponse{ResponseRecorder: httptest.NewRecorder()}
		expected := io.ErrClosedPipe
		switch failure {
		case "deadline":
			response.deadlineError = expected
		case "short":
			response.short, expected = true, io.ErrShortWrite
		case "flush":
			response.flushError = expected
		}
		writer := router.response(response, httptest.NewRequest(http.MethodGet, "/", nil))
		_, err := writer.Write([]byte("media"))
		router.finishResponse(writer)
		router.Close()
		if !errors.Is(err, expected) || !writer.stopped || writer.ctx.Err() == nil {
			test.Fatalf("%s failure hidden: %v", failure, err)
		}
		if _, err := writer.Write([]byte("late")); !errors.Is(err, context.Canceled) {
			test.Fatalf("late write was accepted: %v", err)
		}
	}
}

func TestProxyUpstreamHeaderDeadlineReturnsGatewayFailure(test *testing.T) {
	cancelled := make(chan struct{})
	router := mediaProxy(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		close(cancelled)
	}))
	router.transport.ResponseHeaderTimeout = 50 * time.Millisecond
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/edge/hfs/live/sample.flv", nil))
	if response.Code != http.StatusBadGateway {
		test.Fatalf("header timeout was hidden: %d", response.Code)
	}
	waitProxySignal(test, cancelled, "header timeout retained upstream")
	waitProxyRetirement(test, router)
}

func TestProxyMissingSourceCallbackDeadlinePreservesNotFound(test *testing.T) {
	cancelled := make(chan struct{})
	callback := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		close(cancelled)
	}))
	defer callback.Close()
	proxyConfiguration(test, config.Proxy{Prefix: "edge", OnNotFound: callback.URL})
	router := Init()
	defer router.Close()
	started := time.Now()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/edge/hfs/live/absent.flv", nil))
	if response.Code != http.StatusNotFound || time.Since(started) > 4*time.Second {
		test.Fatalf("callback deadline changed: status=%d duration=%s", response.Code, time.Since(started))
	}
	waitProxySignal(test, cancelled, "callback deadline retained upstream")
	waitProxyRetirement(test, router)
}

func TestProxyCloseReleasesIncompleteIncomingRequestBody(test *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	router := mediaProxy(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		close(entered)
		_, _ = io.Copy(io.Discard, request.Body)
		close(cancelled)
	}))
	server := httptest.NewServer(router)
	defer server.Close()
	connection, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if err != nil {
		test.Fatal(err)
	}
	defer connection.Close()
	_, err = io.WriteString(connection, "GET /edge/hfs/live/sample.flv HTTP/1.1\r\nHost: media\r\nContent-Length: 1048576\r\n\r\nx")
	if err != nil {
		test.Fatal(err)
	}
	waitProxySignal(test, entered, "incomplete request did not reach upstream")
	closed := make(chan struct{})
	go func() { router.Close(); close(closed) }()
	waitProxySignal(test, closed, "shutdown blocked draining incomplete incoming body")
	waitProxySignal(test, cancelled, "incomplete incoming body retained upstream")
	waitProxyRetirement(test, router)
}

type proxyCancellationResponse struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (response *proxyCancellationResponse) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		response.once.Do(func() { close(response.entered) })
		<-response.release
	}
	return nil
}

func TestProxyResponseJoinsCancellationBeforeReturning(test *testing.T) {
	router := Init()
	test.Cleanup(router.Close)
	response := &proxyCancellationResponse{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	var released sync.Once
	test.Cleanup(func() { released.Do(func() { close(response.release) }) })
	ctx, cancel := context.WithCancel(context.Background())
	writer := router.response(response, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	cancel()
	waitProxySignal(test, response.entered, "cancellation did not reach response writer")
	finished := make(chan struct{})
	go func() { router.finishResponse(writer); close(finished) }()
	select {
	case <-finished:
		test.Fatal("request returned before cancellation released the response writer")
	case <-time.After(30 * time.Millisecond):
	}
	released.Do(func() { close(response.release) })
	waitProxySignal(test, finished, "request did not join cancellation")
	if _, err := writer.Write([]byte("late")); !errors.Is(err, context.Canceled) {
		test.Fatalf("response writer reused after return: %v", err)
	}
}

func TestProxyOwnsTransportWithoutClosingSharedPool(test *testing.T) {
	proxyConfiguration(test, config.Proxy{Prefix: "edge"})
	upstream := proxyUpstream(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(response, "ok") }))
	shared := http.DefaultTransport.(*http.Transport)
	router := Init()
	defer router.Close()
	if router.transport == shared {
		test.Fatal("proxy shares transport ownership with unrelated clients")
	}
	client := &http.Client{Transport: shared, Timeout: time.Second}
	var previous net.Conn
	for index := 0; index < 2; index++ {
		var current net.Conn
		request, _ := http.NewRequest(http.MethodGet, upstream.URL, nil)
		request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { current = info.Conn }}))
		response, err := client.Do(request)
		if err != nil {
			test.Fatal(err)
		}
		_, err = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if err != nil {
			test.Fatal(err)
		}
		if index == 0 {
			previous = current
			router.Close()
		} else if previous != current {
			test.Fatal("proxy shutdown closed unrelated shared transport connection")
		}
	}
}
