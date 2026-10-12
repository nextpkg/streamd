package core

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/packet"
)

func TestReaderCloseInterruptsBlockedFlush(test *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	protocol := NewConn(server, ConnBufSize)
	if err := protocol.SetBegin(1); err != nil {
		test.Fatal(err)
	}
	reader := NewReader(NewConnServer(protocol, 1024))
	done := make(chan struct{})
	go func() { reader.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		_ = peer.Close()
		<-done
		test.Fatal("Reader.Close blocked flushing control bytes before closing its transport")
	}
}

type observedAcknowledgement struct {
	net.Conn
	writing chan struct{}
	once    sync.Once
}

func (transport *observedAcknowledgement) Write(body []byte) (int, error) {
	transport.once.Do(func() { close(transport.writing) })
	return transport.Conn.Write(body)
}

func TestReaderCloseInterruptsAcknowledgementLock(test *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	transport := &observedAcknowledgement{Conn: server, writing: make(chan struct{})}
	protocol := NewConn(transport, 1)
	protocol.remoteWindowAckSize = 1
	reader := NewReader(NewConnServer(protocol, 1024))
	reading := make(chan error, 1)
	go func() { reading <- reader.Read(&packet.Packet{}) }()
	if _, err := peer.Write([]byte{6, 0, 0, 0, 0, 0, 1, 9, 1, 0, 0, 0, 0x17}); err != nil {
		test.Fatal(err)
	}
	select {
	case <-transport.writing:
	case <-time.After(time.Second):
		test.Fatal("reader did not attempt its acknowledgement")
	}
	done := make(chan struct{})
	go func() { reader.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		_ = peer.Close()
		<-done
		test.Fatal("Reader.Close waited for the acknowledgement write lock")
	}
	select {
	case err := <-reading:
		if err == nil {
			test.Fatal("aborted acknowledgement returned successful media")
		}
	case <-time.After(300 * time.Millisecond):
		test.Fatal("reader remained blocked after close")
	}
}
