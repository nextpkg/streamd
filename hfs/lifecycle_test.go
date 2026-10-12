package hfs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
)

type lifecycleHandler struct{ install func(base.WriteCloser) }

func (handler *lifecycleHandler) InstallReader(reader base.ReadCloser)  { reader.Close() }
func (handler *lifecycleHandler) InstallWriter(writer base.WriteCloser) { handler.install(writer) }
func (handler *lifecycleHandler) Close()                                {}

type blockingDeadlineResponse struct {
	*httptest.ResponseRecorder
	entered, release chan struct{}
}

func (response *blockingDeadlineResponse) SetWriteDeadline(time.Time) error {
	close(response.entered)
	<-response.release
	return nil
}

func TestHTTPFLVCancellationMonitorFinishesBeforeReturn(test *testing.T) {
	response := &blockingDeadlineResponse{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodGet, "/hfs/origin/sample.flv", nil).WithContext(ctx)
	request.SetPathValue("app", "origin")
	request.SetPathValue("file", "sample.flv")
	installed := make(chan struct{})
	server := &Server{handler: &lifecycleHandler{install: func(writer base.WriteCloser) { <-response.entered; writer.Close(); close(installed) }}}
	returned := make(chan struct{})
	go func() { server.handleConn(response, request); close(returned) }()
	select {
	case <-installed:
	case <-time.After(time.Second):
		test.Fatal("cancellation monitor never reached response deadline")
	}
	select {
	case <-returned:
		test.Error("handler returned while cancellation monitor still owned its ResponseWriter")
	case <-time.After(100 * time.Millisecond):
	}
	close(response.release)
	select {
	case <-returned:
	case <-time.After(time.Second):
		test.Fatal("cancellation monitor was not joined after release")
	}
}

func TestRejectedHTTPFLVDoesNotPoisonKeepAlive(test *testing.T) {
	router := http.NewServeMux()
	NewServer(&lifecycleHandler{install: func(writer base.WriteCloser) { writer.Close() }}).Init(router)
	router.HandleFunc("GET /ready", func(writer http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(writer, "ready") })
	server := httptest.NewServer(router)
	defer server.Close()
	client := server.Client()
	client.Timeout = time.Second
	for repeat := 0; repeat < 256; repeat++ {
		for _, sample := range []struct{ location, body string }{{"/hfs/origin/sample.flv", ""}, {"/ready", "ready"}} {
			response, err := client.Get(server.URL + sample.location)
			if err != nil {
				test.Fatalf("response %d %s: %v", repeat, sample.location, err)
			}
			body, readError := io.ReadAll(response.Body)
			closeError := response.Body.Close()
			if response.StatusCode != http.StatusOK || string(body) != sample.body || readError != nil || closeError != nil {
				test.Fatalf("response %d %s: status=%d body=%q read=%v close=%v", repeat, sample.location, response.StatusCode, body, readError, closeError)
			}
		}
	}
}
