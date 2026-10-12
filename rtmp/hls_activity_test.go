package rtmp

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/rtmp/core"
)

type viewerActivityTerminal struct {
	*core.Universal
	deadline atomic.Int64
}

func (terminal *viewerActivityTerminal) Name() string               { return "viewer-activity" }
func (terminal *viewerActivityTerminal) Write(*packet.Packet) error { return nil }
func (terminal *viewerActivityTerminal) ViewerDeadline() time.Time {
	return time.Unix(0, terminal.deadline.Load())
}

type viewerActivityReader struct {
	*takeoverReader
	expired bool
}

func (reader *viewerActivityReader) IsTimeout() bool { return reader.expired }

func TestOriginHealthRetainsActiveEmbeddedViewer(test *testing.T) {
	app := test.Name()
	config.Get().SetApp(app, &config.App{Enable: true, AppName: app})
	stream := NewStream(base.NewInfo(app, "sample", true))
	stream.active.time = time.Now().Add(-time.Hour).Unix()
	reader := &viewerActivityReader{takeoverReader: &takeoverReader{Universal: core.NewUniversal(stream.info), done: make(chan struct{})}}
	stream.rConn = &StreamReader{ReadCloser: reader}
	terminal := &viewerActivityTerminal{Universal: core.NewUniversal(base.NewInfo(app, "sample", false))}
	terminal.deadline.Store(time.Now().Add(time.Minute).UnixNano())
	writer := core.NewFunnel(terminal)
	defer func() { writer.Close(); writer.Wait() }()
	if err := stream.addWriter(defaultStreamWriter(writer)); err != nil {
		test.Fatal(err)
	}
	if !stream.health() {
		test.Fatal("live embedded viewer treated as an idle origin")
	}
	if stream.client.count.Load() != 0 || stream.active.count.Load() != 0 {
		test.Fatal("HLS activity forged a stateful subscriber")
	}
	reader.expired = true
	if stream.health() {
		test.Fatal("viewer activity masked a dead upstream")
	}
	reader.expired = false
	terminal.deadline.Store(time.Now().Add(-time.Second).UnixNano())
	if stream.health() {
		test.Fatal("expired viewer activity retained the origin")
	}
	terminal.deadline.Store(time.Now().Add(time.Minute).UnixNano())
	writer.Close()
	writer.Wait()
	if stream.health() {
		test.Fatal("closed embedded viewer retained the origin")
	}
}
