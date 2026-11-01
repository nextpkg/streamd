package hls

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/nextpkg/streamd/internal/logging"
)

const responseWriteWindow = 10 * time.Second
const responseWriteChunk = 64 * 1024

type responseWriter struct {
	http.ResponseWriter
	controller    *http.ResponseController
	mutex         sync.Mutex
	timeout       time.Duration
	stopped       bool
	finished      bool
	controlled    bool
	cancelStopped func() bool
	cancelDone    chan struct{}
	done          chan struct{}
}

func newResponseWriter(response http.ResponseWriter, ctx context.Context, timeout time.Duration) *responseWriter {
	if timeout <= 0 {
		timeout = responseWriteWindow
	}
	writer := &responseWriter{ResponseWriter: response, controller: http.NewResponseController(response), timeout: timeout, cancelDone: make(chan struct{}), done: make(chan struct{})}
	writer.cancelStopped = context.AfterFunc(ctx, func() { defer close(writer.cancelDone); writer.abort() })
	return writer
}

func (writer *responseWriter) deadline() error {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	if writer.stopped || writer.finished {
		return context.Canceled
	}
	err := writer.controller.SetWriteDeadline(time.Now().Add(writer.timeout))
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	}
	if err == nil {
		writer.controlled = true
	}
	return err
}

func (writer *responseWriter) Write(body []byte) (int, error) {
	written := 0
	for len(body) > 0 {
		if err := writer.deadline(); err != nil {
			writer.abort()
			return written, err
		}
		chunk := body[:min(len(body), responseWriteChunk)]
		count, err := writer.ResponseWriter.Write(chunk)
		written += count
		if err == nil && count != len(chunk) {
			err = io.ErrShortWrite
		}
		if err == nil {
			err = writer.controller.Flush()
			if errors.Is(err, http.ErrNotSupported) {
				err = nil
			}
		}
		if err != nil {
			writer.abort()
			return written, err
		}
		body = body[len(chunk):]
	}
	return written, nil
}

func (writer *responseWriter) abort() {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	if writer.finished || writer.stopped {
		return
	}
	writer.stopped = true
	_ = writer.controller.SetWriteDeadline(time.Now())
}

func (writer *responseWriter) finish() {
	writer.mutex.Lock()
	writer.finished = true
	if !writer.stopped && writer.controlled {
		if err := writer.controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
			logging.Error("HLS clear response write deadline: ", err)
		}
	}
	writer.mutex.Unlock()
	if !writer.cancelStopped() {
		<-writer.cancelDone
	}
}

func (server *Server) response(response http.ResponseWriter, request *http.Request) *responseWriter {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.closed {
		return nil
	}
	writer := newResponseWriter(response, request.Context(), server.responseTimeout)
	if server.responses == nil {
		server.responses = make(map[*responseWriter]struct{})
	}
	server.responses[writer] = struct{}{}
	return writer
}

func (server *Server) finishResponse(writer *responseWriter) {
	writer.finish()
	server.mu.Lock()
	delete(server.responses, writer)
	server.mu.Unlock()
	close(writer.done)
}
