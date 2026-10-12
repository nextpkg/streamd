package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/metrics"
)

const responseWriteWindow = 10 * time.Second
const responseWriteChunk = 32 * 1024

type responseWriter struct {
	http.ResponseWriter
	controller    *http.ResponseController
	ctx           context.Context
	cancel        context.CancelFunc
	mutex         sync.Mutex
	timeout       time.Duration
	stopped       bool
	finished      bool
	controlled    bool
	cancelStopped func() bool
	cancelDone    chan struct{}
	done          chan struct{}
}

func (server *Server) response(response http.ResponseWriter, request *http.Request) *responseWriter {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.closed {
		return nil
	}
	timeout := server.responseTimeout
	if timeout <= 0 {
		timeout = responseWriteWindow
	}
	ctx, cancel := context.WithCancel(request.Context())
	writer := &responseWriter{ResponseWriter: response, controller: http.NewResponseController(response), ctx: ctx, cancel: cancel, timeout: timeout, cancelDone: make(chan struct{}), done: make(chan struct{})}
	writer.cancelStopped = context.AfterFunc(ctx, func() { defer close(writer.cancelDone); writer.abort() })
	server.requests[writer] = struct{}{}
	metrics.ProxyRequestStarted()
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
			err = writer.FlushError()
		}
		if err != nil {
			writer.fail(err)
			return written, err
		}
		body = body[len(chunk):]
	}
	return written, nil
}

func (writer *responseWriter) FlushError() error {
	if err := writer.deadline(); err != nil {
		writer.abort()
		return err
	}
	err := writer.controller.Flush()
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	}
	if err != nil {
		writer.fail(err)
	}
	return err
}

func (writer *responseWriter) Flush() { _ = writer.FlushError() }

func (writer *responseWriter) fail(err error) {
	if writer.ctx.Err() == nil {
		log.Warn("proxy response failed: ", err)
	}
	writer.abort()
}

func (writer *responseWriter) abort() {
	writer.mutex.Lock()
	if !writer.finished && !writer.stopped {
		writer.stopped = true
		_ = writer.controller.SetWriteDeadline(time.Now())
		_ = writer.controller.SetReadDeadline(time.Now())
	}
	writer.mutex.Unlock()
	writer.cancel()
}

func (server *Server) finishResponse(writer *responseWriter) {
	_ = writer.FlushError()
	writer.mutex.Lock()
	writer.finished = true
	if !writer.stopped && writer.controlled {
		if err := writer.controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
			log.Error("proxy clear response write deadline: ", err)
		}
	}
	writer.mutex.Unlock()
	if !writer.cancelStopped() {
		<-writer.cancelDone
	}
	writer.cancel()
	server.mutex.Lock()
	delete(server.requests, writer)
	metrics.ProxyRequestFinished(writer.stopped)
	server.mutex.Unlock()
	close(writer.done)
}
