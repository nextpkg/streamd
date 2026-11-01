package console

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/rtmp"
)

type blockedQueryWriter struct {
	header        http.Header
	started       chan struct{}
	released      chan struct{}
	interrupted   chan struct{}
	interruptOnce sync.Once
	mu            sync.Mutex
	writeDeadline time.Time
	readDeadline  time.Time
}

func (writer *blockedQueryWriter) Header() http.Header { return writer.header }
func (writer *blockedQueryWriter) WriteHeader(int)     {}
func (writer *blockedQueryWriter) Write(body []byte) (int, error) {
	close(writer.started)
	select {
	case <-writer.interrupted:
		return 0, context.Canceled
	case <-writer.released:
		return len(body), nil
	}
}
func (writer *blockedQueryWriter) SetWriteDeadline(deadline time.Time) error {
	writer.mu.Lock()
	writer.writeDeadline = deadline
	writer.mu.Unlock()
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		writer.interruptOnce.Do(func() { close(writer.interrupted) })
	}
	return nil
}
func (writer *blockedQueryWriter) SetReadDeadline(deadline time.Time) error {
	writer.mu.Lock()
	writer.readDeadline = deadline
	writer.mu.Unlock()
	return nil
}
func (writer *blockedQueryWriter) FlushError() error {
	select {
	case <-writer.interrupted:
		return context.Canceled
	default:
		return nil
	}
}

func TestAdminQueryCancellationAndCloseRetireBlockedResponses(test *testing.T) {
	for _, mode := range []string{"request", "server"} {
		test.Run(mode, func(test *testing.T) {
			connection := rtmp.NewConn(context.Background())
			defer connection.Close()
			admin := NewServer(connection)
			router := admin.Init(http.NewServeMux())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			writer := &blockedQueryWriter{header: make(http.Header), started: make(chan struct{}), released: make(chan struct{}), interrupted: make(chan struct{})}
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				router.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/admin/stat/keys", nil).WithContext(ctx))
			}()
			defer func() { close(writer.released); <-finished; admin.Close() }()
			select {
			case <-writer.started:
			case <-time.After(time.Second):
				test.Fatal("query did not start")
			}
			writer.mu.Lock()
			writeDeadline, readDeadline := writer.writeDeadline, writer.readDeadline
			writer.mu.Unlock()
			if writeDeadline.IsZero() || readDeadline.IsZero() || time.Until(writeDeadline) > 6*time.Second {
				test.Error("management response has no bounded I/O deadline")
			}
			closed := make(chan struct{})
			if mode == "request" {
				cancel()
				close(closed)
			} else {
				go func() { admin.Close(); close(closed) }()
			}
			select {
			case <-finished:
			case <-time.After(300 * time.Millisecond):
				test.Error("cancelled management query kept blocked response alive")
			}
			select {
			case <-closed:
			case <-time.After(300 * time.Millisecond):
				test.Error("management close did not finish")
			}
		})
	}
}

func TestClosedAdminRejectsReadOnlyQueries(test *testing.T) {
	admin := NewServer(nil)
	router := admin.Init(http.NewServeMux())
	admin.Close()
	response := httptest.NewRecorder()
	defer func() {
		if recovered := recover(); recovered != nil {
			test.Errorf("closed query still reached handler: %v", recovered)
		}
	}()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/stat/keys", nil))
	if response.Code != http.StatusServiceUnavailable {
		test.Fatal(errors.New("closed console admitted a new query"))
	}
}

type completedQueryWriter struct {
	*httptest.ResponseRecorder
	events []string
}

func (writer *completedQueryWriter) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		writer.events = append(writer.events, "read-clear")
	} else {
		writer.events = append(writer.events, "read-bound")
	}
	return nil
}
func (writer *completedQueryWriter) SetWriteDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		writer.events = append(writer.events, "write-clear")
	} else {
		writer.events = append(writer.events, "write-bound")
	}
	return nil
}
func (writer *completedQueryWriter) FlushError() error {
	writer.events = append(writer.events, "flush")
	writer.Flush()
	return nil
}

func TestAdminSuccessfulQueryFlushesBeforeClearingDeadlines(test *testing.T) {
	connection := rtmp.NewConn(context.Background())
	defer connection.Close()
	admin := NewServer(connection)
	defer admin.Close()
	writer := &completedQueryWriter{ResponseRecorder: httptest.NewRecorder()}
	admin.Init(http.NewServeMux()).ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/admin/stat/keys", nil))
	expected := []string{"read-bound", "write-bound", "flush", "read-clear", "write-clear"}
	if len(writer.events) != len(expected) {
		test.Fatalf("deadline order: %v", writer.events)
	}
	for index, event := range expected {
		if writer.events[index] != event {
			test.Fatalf("deadline order: %v", writer.events)
		}
	}
	if writer.Code != 200 {
		test.Fatal(writer.Code)
	}
}

func TestAdminQueryConcurrentCloseAndAdmission(test *testing.T) {
	connection := rtmp.NewConn(context.Background())
	defer connection.Close()
	admin := NewServer(connection)
	server := httptest.NewServer(admin.Init(http.NewServeMux()))
	defer server.Close()
	var workers sync.WaitGroup
	started := make(chan struct{})
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-started
			for range 3 {
				response, err := server.Client().Get(server.URL + "/admin/stat/keys")
				if err != nil {
					test.Error(err)
					return
				}
				_, err = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if err != nil {
					test.Error(err)
					return
				}
				if response.StatusCode != 200 && response.StatusCode != 503 {
					test.Errorf("unexpected status %d", response.StatusCode)
				}
			}
		}()
	}
	for range 8 {
		workers.Add(1)
		go func() { defer workers.Done(); <-started; admin.Close() }()
	}
	close(started)
	workers.Wait()
	admin.Close()
}

func TestAdminCloseJoinsAllAcceptedQueriesAndRejectsLateAdmission(test *testing.T) {
	connection := rtmp.NewConn(context.Background())
	defer connection.Close()
	admin := NewServer(connection)
	router := admin.Init(http.NewServeMux())
	var handlers sync.WaitGroup
	writers := []*blockedQueryWriter{}
	defer func() {
		for _, writer := range writers {
			close(writer.released)
		}
		handlers.Wait()
		admin.Close()
	}()
	for range 32 {
		writer := &blockedQueryWriter{header: make(http.Header), started: make(chan struct{}), released: make(chan struct{}), interrupted: make(chan struct{})}
		writers = append(writers, writer)
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			router.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/admin/stat/keys", nil))
		}()
		select {
		case <-writer.started:
		case <-time.After(time.Second):
			test.Fatal("admitted query failed to start")
		}
	}
	closed := make(chan struct{})
	go func() {
		var closers sync.WaitGroup
		for range 8 {
			closers.Add(1)
			go func() { defer closers.Done(); admin.Close() }()
		}
		closers.Wait()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		test.Fatal("close did not join admitted queries")
	}
	for _, writer := range writers {
		select {
		case <-writer.interrupted:
		default:
			test.Fatal("admitted query escaped cancellation")
		}
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/stat/keys", nil))
	if response.Code != 503 {
		test.Fatal("late query was admitted")
	}
}
