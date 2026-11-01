package hls

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/hls/item"
)

type flushFailedResponse struct{ *httptest.ResponseRecorder }

func (response *flushFailedResponse) FlushError() error { return io.ErrClosedPipe }

func TestHLSFlushFailureDoesNotRefreshViewer(test *testing.T) {
	for _, suffix := range []string{"sample.m3u8", "sample/1.ts"} {
		test.Run(suffix, func(test *testing.T) {
			_, stream, router := activityStream(test, false)
			response := &flushFailedResponse{ResponseRecorder: httptest.NewRecorder()}
			request := httptest.NewRequest(http.MethodGet, "/hls/"+stream.Info().App+"/"+suffix, nil)
			router.ServeHTTP(response, request)
			if !stream.ViewerDeadline().IsZero() {
				test.Fatal("failed final flush refreshed HLS viewer activity")
			}
		})
	}
}

type progressingResponse struct {
	*httptest.ResponseRecorder
	deadline  time.Time
	deadlines []time.Time
	writes    int
	delay     time.Duration
}

func (response *progressingResponse) SetWriteDeadline(deadline time.Time) error {
	response.deadline = deadline
	response.deadlines = append(response.deadlines, deadline)
	return nil
}

func (response *progressingResponse) Write(body []byte) (int, error) {
	if len(body) > responseWriteChunk {
		return 0, errors.New("response copied an unbounded chunk")
	}
	time.Sleep(response.delay)
	if !response.deadline.After(time.Now()) {
		return 0, os.ErrDeadlineExceeded
	}
	response.writes++
	return response.ResponseRecorder.Write(body)
}

func TestHLSWriteDeadlineRenewsWithProgressAndClearsAfterFlush(test *testing.T) {
	response := &progressingResponse{ResponseRecorder: httptest.NewRecorder(), delay: 30 * time.Millisecond}
	writer := newResponseWriter(response, context.Background(), 100*time.Millisecond)
	payload := bytes.Repeat([]byte{0x47, 3, 1}, responseWriteChunk*2)
	started := time.Now()
	written, err := writer.Write(payload)
	writer.finish()
	if err != nil || written != len(payload) || !bytes.Equal(payload, response.Body.Bytes()) || response.writes != 6 || time.Since(started) <= writer.timeout {
		test.Fatalf("progressive delivery failed: written=%d writes=%d error=%v", written, response.writes, err)
	}
	if len(response.deadlines) != 7 || !response.deadlines[6].IsZero() {
		test.Fatal("healthy response did not clear deadline")
	}
	for index := 1; index < 6; index++ {
		if !response.deadlines[index].After(response.deadlines[index-1]) {
			test.Fatal("deadline was not renewed after progress")
		}
	}
	writer.abort()
	if len(response.deadlines) != 7 {
		test.Fatal("late cancellation changed a finished response")
	}
}

type joinedCancellationResponse struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
}

func (response *joinedCancellationResponse) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		close(response.entered)
		<-response.release
	}
	return nil
}

func TestHLSResponseJoinsCancellationBeforeReturning(test *testing.T) {
	response := &joinedCancellationResponse{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	writer := newResponseWriter(response, ctx, time.Second)
	cancel()
	select {
	case <-response.entered:
	case <-time.After(time.Second):
		test.Fatal("cancellation did not reach response")
	}
	done := make(chan struct{})
	go func() { writer.finish(); close(done) }()
	select {
	case <-done:
		test.Fatal("response returned while cancellation still owned its writer")
	case <-time.After(30 * time.Millisecond):
	}
	close(response.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		test.Fatal("cancellation not joined")
	}
	if _, err := writer.Write([]byte("late")); !errors.Is(err, context.Canceled) {
		test.Fatalf("late write accepted: %v", err)
	}
}

func TestHLSSlowTCPResponseExpiresWithoutClientResumingRead(test *testing.T) {
	server, stream, router := activityStream(test, false)
	server.responseTimeout = 500 * time.Millisecond
	payload := bytes.Repeat([]byte{0x47, 1, 2, 3}, 2*1024*1024)
	stream.m3u8.SetItem("large", item.NewTsItem("sample/large.ts", 4000, 3, payload))
	closed := make(chan struct{})
	var closeOnce sync.Once
	httpServer := httptest.NewUnstartedServer(router)
	httpServer.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closeOnce.Do(func() { close(closed) })
		}
	}
	httpServer.Start()
	test.Cleanup(httpServer.Close)
	transport, err := net.Dial("tcp", httpServer.Listener.Addr().String())
	if err != nil {
		test.Fatal(err)
	}
	connection := transport.(*net.TCPConn)
	test.Cleanup(func() { _ = connection.Close() })
	if err := connection.SetReadBuffer(1024); err != nil {
		test.Fatal(err)
	}
	if err := connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		test.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, httpServer.URL+"/hls/"+stream.Info().App+"/sample/large.ts", nil)
	if err != nil {
		test.Fatal(err)
	}
	if err := request.Write(connection); err != nil {
		test.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), request)
	if err != nil {
		test.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || response.ContentLength != int64(len(payload)) {
		test.Fatal("unexpected segment response")
	}
	if _, err := io.ReadFull(response.Body, make([]byte, 188)); err != nil {
		test.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		test.Fatal("slow HTTP connection stayed open without resumed reads")
	}
	server.mu.Lock()
	remaining := len(server.responses)
	server.mu.Unlock()
	if remaining != 0 || !stream.ViewerDeadline().IsZero() {
		test.Fatal("slow response retained ownership or fabricated activity")
	}
	healthy, err := httpServer.Client().Get(request.URL.String())
	if err != nil {
		test.Fatal(err)
	}
	body, readError := io.ReadAll(healthy.Body)
	closeError := healthy.Body.Close()
	if readError != nil || closeError != nil || !bytes.Equal(body, payload) {
		test.Fatalf("slow failure changed cached media: read=%v close=%v bytes=%d", readError, closeError, len(body))
	}
	if stream.ViewerDeadline().IsZero() {
		test.Fatal("successful response failed to refresh activity")
	}
}

func TestHLSHealthyResponsesKeepConnectionReusable(test *testing.T) {
	server, stream, router := activityStream(test, false)
	server.responseTimeout = 30 * time.Millisecond
	httpServer := httptest.NewServer(router)
	defer httpServer.Close()
	connection, err := net.DialTimeout("tcp", httpServer.Listener.Addr().String(), time.Second)
	if err != nil {
		test.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	reader := bufio.NewReader(connection)
	for index := 0; index < 8; index++ {
		if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
			test.Fatal(err)
		}
		request, err := http.NewRequest(http.MethodGet, httpServer.URL+"/hls/"+stream.Info().App+"/sample/1.ts?repeat="+strconv.Itoa(index), nil)
		if err != nil {
			test.Fatal(err)
		}
		if err := request.Write(connection); err != nil {
			test.Fatal(err)
		}
		response, err := http.ReadResponse(reader, request)
		if err != nil {
			test.Fatal(err)
		}
		body, readError := io.ReadAll(response.Body)
		closeError := response.Body.Close()
		if readError != nil || closeError != nil || !bytes.Equal(body, []byte{0x47, 1, 2, 3}) || response.Close {
			test.Fatalf("reused response failed: %v %v", readError, closeError)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

type failingBoundedResponse struct {
	*httptest.ResponseRecorder
	deadlineError error
	short         bool
	writes        int
}

func (response *failingBoundedResponse) SetWriteDeadline(time.Time) error {
	return response.deadlineError
}
func (response *failingBoundedResponse) Write(body []byte) (int, error) {
	response.writes++
	if response.short {
		return len(body) - 1, nil
	}
	return response.ResponseRecorder.Write(body)
}

func TestHLSResponsePropagatesDeadlineAndShortWriteFailures(test *testing.T) {
	for _, short := range []bool{false, true} {
		response := &failingBoundedResponse{ResponseRecorder: httptest.NewRecorder(), short: short}
		expected := io.ErrShortWrite
		if !short {
			response.deadlineError, expected = io.ErrClosedPipe, io.ErrClosedPipe
		}
		writer := newResponseWriter(response, context.Background(), time.Second)
		_, err := writer.Write([]byte{1, 2, 3})
		writer.finish()
		if !errors.Is(err, expected) || !writer.stopped || (!short && response.writes != 0) {
			test.Fatalf("failure hidden: writes=%d err=%v", response.writes, err)
		}
	}
}

func TestConcurrentHLSServerCloseWaitsForResponsesAndRejectsLateRequests(test *testing.T) {
	server, stream, router := activityStream(test, false)
	var requests sync.WaitGroup
	for index := 0; index < 4; index++ {
		response := &deadlineBlockedResponse{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
		test.Cleanup(func() { response.stop.Do(func() { close(response.release) }) })
		requests.Add(1)
		go func() {
			defer requests.Done()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/hls/"+stream.Info().App+"/sample/1.ts", nil))
		}()
		select {
		case <-response.entered:
		case <-time.After(time.Second):
			test.Fatal("response did not start")
		}
	}
	var closers sync.WaitGroup
	for index := 0; index < 16; index++ {
		closers.Add(1)
		go func() { defer closers.Done(); server.Close() }()
	}
	done := make(chan struct{})
	go func() { closers.Wait(); requests.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		test.Fatal("concurrent shutdown did not join all response owners")
	}
	server.mu.Lock()
	remaining := len(server.responses)
	server.mu.Unlock()
	if remaining != 0 {
		test.Fatal("finished responses retained")
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/hls/"+stream.Info().App+"/sample.m3u8", nil))
	if response.Code != http.StatusServiceUnavailable {
		test.Fatalf("late response status=%d", response.Code)
	}
}

type deadlineBlockedResponse struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
	start   sync.Once
	stop    sync.Once
}

func (response *deadlineBlockedResponse) Write([]byte) (int, error) {
	response.start.Do(func() { close(response.entered) })
	<-response.release
	return 0, io.ErrClosedPipe
}

func (response *deadlineBlockedResponse) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		response.stop.Do(func() { close(response.release) })
	}
	return nil
}

func TestHLSServerCloseInterruptsBlockedResponse(test *testing.T) {
	server, stream, router := activityStream(test, false)
	response := &deadlineBlockedResponse{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/hls/"+stream.Info().App+"/sample/1.ts", nil))
		close(done)
	}()
	test.Cleanup(func() { response.stop.Do(func() { close(response.release) }); <-done })
	select {
	case <-response.entered:
	case <-time.After(time.Second):
		test.Fatal("response did not start")
	}
	closed := make(chan struct{})
	go func() { server.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		test.Fatal("HLS Close did not interrupt its blocked response")
	}
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		test.Fatal("HLS Close returned while the blocked response retained its segment")
	}
	if !stream.ViewerDeadline().IsZero() {
		test.Fatal("aborted response refreshed viewer activity")
	}
}
