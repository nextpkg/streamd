package console

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/nextpkg/streamd/internal/httpx"
)

const queryIOBudget = 5 * time.Second

func (server *Server) closeQueries() {
	server.queryMu.Lock()
	server.queryClosed = true
	if server.queryCancel != nil {
		server.queryCancel()
	}
	server.queryMu.Unlock()
	server.queryWait.Wait()
}

func (server *Server) managementRequest(handler http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		controller := http.NewResponseController(writer)
		server.queryMu.Lock()
		if server.queryClosed {
			server.queryMu.Unlock()
			_ = controller.SetReadDeadline(time.Now())
			_ = controller.SetWriteDeadline(time.Now().Add(queryIOBudget))
			writer.Header().Set("Connection", "close")
			httpx.NewWriter(writer).QuickSendJSON(http.StatusServiceUnavailable, "console is shutting down", nil)
			return
		}
		if server.queryContext == nil {
			server.queryContext, server.queryCancel = context.WithCancel(context.Background())
		}
		ctx, cancel := context.WithTimeout(request.Context(), queryIOBudget)
		stopOwner := context.AfterFunc(server.queryContext, cancel)
		server.queryWait.Add(1)
		server.queryMu.Unlock()
		defer server.queryWait.Done()
		defer cancel()
		defer stopOwner()
		deadline := time.Now().Add(queryIOBudget)
		if err := controller.SetReadDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return
		}
		if err := controller.SetWriteDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return
		}
		cancelled := make(chan struct{})
		stopIO := context.AfterFunc(ctx, func() {
			_ = controller.SetReadDeadline(time.Now())
			_ = controller.SetWriteDeadline(time.Now())
			close(cancelled)
		})
		defer func() {
			var bodyError error
			if request.Body != nil {
				bodyError = request.Body.Close()
			}
			flushError := controller.Flush()
			if !stopIO() {
				<-cancelled
			}
			if ctx.Err() == nil && bodyError == nil && (flushError == nil || errors.Is(flushError, http.ErrNotSupported)) {
				_ = controller.SetReadDeadline(time.Time{})
				_ = controller.SetWriteDeadline(time.Time{})
			}
		}()
		handler(writer, request.WithContext(ctx))
	}
}
