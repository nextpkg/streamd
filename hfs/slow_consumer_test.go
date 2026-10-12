package hfs

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
)

type stalledFLVResponse struct {
	*httptest.ResponseRecorder
	entered   chan struct{}
	release   chan struct{}
	start     sync.Once
	stop      sync.Once
	deadlines []time.Time
}

func (response *stalledFLVResponse) Write([]byte) (int, error) {
	response.start.Do(func() { close(response.entered) })
	<-response.release
	return 0, net.ErrClosed
}

func (response *stalledFLVResponse) SetWriteDeadline(deadline time.Time) error {
	response.deadlines = append(response.deadlines, deadline)
	response.stop.Do(func() { close(response.release) })
	return nil
}

func TestHTTPFLVAbortInterruptsWriteWithoutResettingExpiredDeadline(test *testing.T) {
	response := &stalledFLVResponse{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	writer := NewStream("live", test.Name(), response)
	test.Cleanup(func() { response.stop.Do(func() { close(response.release) }); writer.Abort(); writer.Wait() })
	frame := &packet.Packet{Type: packet.PktAudio, Data: []byte{0xaf, 1, 1}}
	if err := flv.NewDemuxer().Demux(frame); err != nil {
		test.Fatal(err)
	}
	if err := writer.Write(frame); err != nil {
		test.Fatal(err)
	}
	select {
	case <-response.entered:
	case <-time.After(time.Second):
		test.Fatal("HTTP writer did not block")
	}
	writer.Abort()
	done := make(chan struct{})
	go func() { writer.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		test.Fatal("HTTP writer did not retire")
	}
	writer.Terminal.(*Stream).Abort()
	if len(response.deadlines) != 1 || response.deadlines[0].IsZero() || response.deadlines[0].After(time.Now()) {
		test.Fatalf("abort failed to expire response or revived its final flush: %v", response.deadlines)
	}
}

func TestHTTPFLVIdleAbortDoesNotExpireUnwrittenResponse(test *testing.T) {
	response := &stalledFLVResponse{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	writer := NewStream("live", test.Name(), response)
	writer.Abort()
	writer.Wait()
	if len(response.deadlines) != 0 {
		test.Fatal("idle response deadline poisoned keepalive")
	}
	if _, err := response.ResponseRecorder.WriteString(http.StatusText(http.StatusOK)); err != nil {
		test.Fatal(err)
	}
}
