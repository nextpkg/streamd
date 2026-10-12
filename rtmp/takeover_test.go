package rtmp

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/rtmp/core"
	"github.com/stretchr/testify/require"
)

type takeoverReader struct {
	*core.Universal
	done chan struct{}
	once sync.Once
}

func (reader *takeoverReader) Read(frame *packet.Packet) error { <-reader.done; return io.EOF }
func (reader *takeoverReader) Close()                          { reader.once.Do(func() { close(reader.done) }) }
func (reader *takeoverReader) IsTimeout() bool                 { return false }
func (reader *takeoverReader) GetPublish() *base.PublishInfo {
	return &base.PublishInfo{Name: reader.Info().Instance}
}
func (reader *takeoverReader) GetConnect() *base.ConnectInfo {
	return &base.ConnectInfo{App: reader.Info().App}
}

type takeoverTerminal struct{ *core.Universal }

func (terminal *takeoverTerminal) Name() string                     { return "takeover-test" }
func (terminal *takeoverTerminal) Write(frame *packet.Packet) error { return nil }

func TestConcurrentPublishersTransferOneWriterAndReleaseOldReaders(t *testing.T) {
	app := t.Name()
	config.Get().SetApp(app, &config.App{Enable: true, AppName: app})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := NewConn(ctx)
	defer connection.Close()
	makeReader := func() *takeoverReader {
		return &takeoverReader{Universal: core.NewUniversal(base.NewInfo(app, "sample", false)), done: make(chan struct{})}
	}
	initial := makeReader()
	connection.InstallReader(initial)
	writer := core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(base.NewInfo(app, "sample", true))})
	defer func() { writer.Close(); writer.Wait() }()
	connection.InstallWriter(writer)
	readers := []*takeoverReader{initial}
	var workers sync.WaitGroup
	for index := 0; index < 12; index++ {
		reader := makeReader()
		readers = append(readers, reader)
		workers.Add(1)
		go func() { defer workers.Done(); connection.InstallReader(reader) }()
	}
	workers.Wait()
	closed := 0
	for _, reader := range readers {
		select {
		case <-reader.done:
			closed++
		default:
		}
	}
	require.Equal(t, len(readers)-1, closed)
	require.False(t, writer.IsClosed())
	count, _ := connection.WriterActive(initial.Info().Key)
	require.Equal(t, int64(1), count)
	value, exists := connection.stream.Load(initial.Info().Key)
	require.True(t, exists)
	require.Len(t, value.(*Stream).Writer(), 1)
	require.False(t, value.(*Stream).info.IsExternal)
	connection.Close()
	require.Empty(t, connection.StreamKeys())
	require.True(t, writer.IsClosed())
	late := makeReader()
	connection.InstallReader(late)
	select {
	case <-late.done:
	default:
		t.Fatal("publisher accepted after shutdown")
	}
}
