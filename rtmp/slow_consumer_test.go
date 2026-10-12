package rtmp

import (
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/rtmp/core"
)

type blockedViewer struct {
	*core.Universal
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	started sync.Once
}

func (viewer *blockedViewer) Name() string { return "blocked-viewer" }
func (viewer *blockedViewer) Write(*packet.Packet) error {
	viewer.started.Do(func() { close(viewer.entered) })
	<-viewer.release
	return core.ErrWriterWasCanceled
}
func (viewer *blockedViewer) Abort() { viewer.once.Do(func() { close(viewer.release) }) }

type countingViewer struct {
	*idleWriter
	packets int
}

func (viewer *countingViewer) Write(*packet.Packet) error { viewer.packets++; return nil }

func TestSaturatedViewerRetiresWithoutBlockingHealthyViewer(test *testing.T) {
	stream := newStream(base.NewInfo("slow-consumer", test.Name(), false), config.Get(), &config.App{})
	terminal := &blockedViewer{Universal: core.NewUniversal(base.NewInfo("slow-consumer", test.Name(), true)), entered: make(chan struct{}), release: make(chan struct{})}
	slow := core.NewFunnel(terminal)
	test.Cleanup(func() { terminal.Abort(); slow.Close(); slow.Wait() })
	healthy := &countingViewer{idleWriter: &idleWriter{Active: base.NewRwAlive(), info: base.NewInfo("slow-consumer", test.Name(), true)}}
	for _, viewer := range []base.WriteCloser{slow, healthy} {
		writer := defaultStreamWriter(viewer)
		writer.hasGOP = true
		if err := stream.addWriter(writer); err != nil {
			test.Fatal(err)
		}
	}
	test.Cleanup(func() { _ = stream.terminate(true) })
	frame := &packet.Packet{Type: packet.PktAudio, Data: []byte{0xaf, 1, 1}}
	if err := flv.NewDemuxer().Demux(frame); err != nil {
		test.Fatal(err)
	}
	stream.dispatch(frame)
	select {
	case <-terminal.entered:
	case <-time.After(time.Second):
		test.Fatal("slow terminal never started")
	}
	for index := 0; index < 4097; index++ {
		stream.dispatch(frame)
	}
	if healthy.packets != 4098 || stream.client.count.Load() != 1 || len(stream.Writer()) != 1 {
		test.Fatalf("saturated viewer affected healthy dispatch: packets=%d viewers=%d", healthy.packets, stream.client.count.Load())
	}
	done := make(chan struct{})
	go func() { slow.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		test.Fatal("saturated viewer removed from statistics but its blocked writer never retired")
	}
	stream.dispatch(frame)
	if healthy.packets != 4099 {
		test.Fatal("healthy viewer did not continue after slow viewer retirement")
	}
}
