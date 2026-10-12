package core

import (
	"io"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/packet"
)

type readerPrefetchTransport struct {
	chunks       []ChunkStream
	reads        int
	closes       int
	flushFailure error
}

func (transport *readerPrefetchTransport) Read(chunk *ChunkStream) error {
	if transport.reads >= len(transport.chunks) {
		return io.EOF
	}
	*chunk = transport.chunks[transport.reads]
	transport.reads++
	return nil
}
func (transport *readerPrefetchTransport) Write(*ChunkStream) error  { return nil }
func (transport *readerPrefetchTransport) Flush() error              { return transport.flushFailure }
func (transport *readerPrefetchTransport) Close() error              { transport.closes++; return nil }
func (transport *readerPrefetchTransport) GetInfo() (string, string) { return "live", "sample" }
func (transport *readerPrefetchTransport) GetPublish() *base.PublishInfo {
	return &base.PublishInfo{Name: "sample"}
}
func (transport *readerPrefetchTransport) GetConnect() *base.ConnectInfo {
	return &base.ConnectInfo{App: "live"}
}

func TestReaderPrefetchRetainsPayloadAndSingleAccounting(test *testing.T) {
	first, second := []byte{0x17, 1, 0, 0, 0, 1}, []byte{0x27, 1, 0, 0, 0, 2}
	transport := &readerPrefetchTransport{chunks: []ChunkStream{{typeID: 9, timestamp: 80, data: first}, {typeID: 9, timestamp: 120, data: second}}}
	reader := NewReader(transport)
	for repeat := 0; repeat < 2; repeat++ {
		if err := reader.Prefetch(); err != nil {
			test.Fatal(err)
		}
	}
	if transport.reads != 1 || reader.VideoLen() != uint64(len(first)) {
		test.Fatal("prefetch read ahead or counted twice")
	}
	frame := &packet.Packet{}
	if err := reader.Read(frame); err != nil {
		test.Fatal(err)
	}
	if transport.reads != 1 || frame.TimeStamp != 80 || &frame.Data[0] != &first[0] || reader.VideoLen() != uint64(len(first)) {
		test.Fatal("prefetch lost the first frame, copied its payload, or duplicated statistics")
	}
	if err := reader.Read(frame); err != nil {
		test.Fatal(err)
	}
	if frame.TimeStamp != 120 || &frame.Data[0] != &second[0] || reader.VideoLen() != uint64(len(first)+len(second)) {
		test.Fatal("stream did not continue after prefetch")
	}
}

func TestReaderCloseReleasesTransportAfterFlushFailure(test *testing.T) {
	transport := &readerPrefetchTransport{flushFailure: io.ErrClosedPipe}
	NewReader(transport).Close()
	if transport.closes != 1 {
		test.Fatal("flush failure leaked the origin transport")
	}
}
