package rtmp

import (
	"sync"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/rtmp/core"
)

type idleWriter struct {
	*base.Active
	info *base.Info
}

func TestPublisherStatisticsExcludeTerminatedStream(test *testing.T) {
	stream := NewStream(base.NewInfo("live", "sample", false))
	reader := &core.Reader{}
	stream.rConn = &StreamReader{ReadCloser: reader}
	connection := &Conn{}
	connection.stream.Store(stream.info.Key, stream)
	if connection.Reader(stream.info.Key) != reader {
		test.Fatal("active publisher missing from statistics")
	}
	stream.inside.control.Cancel()
	if connection.Reader(stream.info.Key) != nil || len(connection.StreamKeys()) != 1 {
		test.Fatal("publisher statistics wait for cache eviction rather than stream termination")
	}
}

func (writer *idleWriter) Info() *base.Info           { return writer.info }
func (writer *idleWriter) Write(*packet.Packet) error { return nil }
func (writer *idleWriter) Close()                     {}

func TestConcurrentWriterRegistrationAndStatistics(t *testing.T) {
	config.Get().SetApp("concurrent", &config.App{Enable: true, AppName: "concurrent"})
	stream := NewStream(base.NewInfo("concurrent", "sample", false))
	connection := &Conn{}
	connection.stream.Store(stream.info.Key, stream)
	var workers sync.WaitGroup
	for index := 0; index < 64; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			writer := &idleWriter{Active: base.NewRwAlive(), info: base.NewInfo("concurrent", "sample", true)}
			if err := stream.addWriter(defaultStreamWriter(writer)); err != nil {
				t.Error(err)
			}
			_, _ = connection.WriterActive(stream.info.Key)
		}()
	}
	workers.Wait()
	count, checkin := connection.WriterActive(stream.info.Key)
	if count != 64 || checkin == 0 {
		t.Fatalf("statistics = %d, %d", count, checkin)
	}
	for _, writer := range stream.Writer() {
		stream.delWriter(writer, writer.Info().UID)
		stream.delWriter(writer, writer.Info().UID)
	}
	if count, _ := connection.WriterActive(stream.info.Key); count != 0 {
		t.Fatalf("duplicate removal corrupted counter: %d", count)
	}
}
