package core

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestOriginClientAllocationOptionsAreSessionScoped(test *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	for _, options := range []ClientOptions{{BufferSize: 8192, SlabMin: 32 * 1024, SlabMax: 128 * 1024}, {BufferSize: 4096, SlabMin: 64 * 1024, SlabMax: 512 * 1024}} {
		client, err := DialConnClient(context.Background(), "rtmp://"+listener.Addr().String()+"/source/sample", time.Second, options)
		if err != nil {
			test.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		if client.conn.rw.Reader.Size() != options.BufferSize || client.conn.rw.Writer.Size() != options.BufferSize || client.conn.slab.mark.min != options.SlabMin || client.conn.slab.mark.max != options.SlabMax {
			test.Fatal("origin connection lost its configured buffer/slab settings")
		}
		if len(client.conn.slab.Get(1024)) != 1024 {
			test.Fatal("configured origin allocator cannot serve a media packet")
		}
	}
}

func TestServerChunkSizeIsSessionScoped(test *testing.T) {
	first, second := net.Pipe()
	test.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	firstSession := NewConnServer(NewConn(first, ConnBufSize), 1024)
	secondSession := NewConnServer(NewConn(second, ConnBufSize), 4096)
	if firstSession.conn.chunkSize != 1024 || secondSession.conn.chunkSize != 4096 {
		test.Fatal("server sessions did not retain independent chunk sizes")
	}
}

func TestInvalidClientPathDoesNotOpenConnection(test *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = listener.Close() })
	client := NewConnClientByURL("rtmp://"+listener.Addr().String()+"/missing-stream", time.Second)
	if client != nil {
		test.Fatal("invalid client path was accepted")
	}
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		test.Fatal(err)
	}
	connection, err := listener.Accept()
	if err == nil {
		_ = connection.Close()
		test.Fatal("invalid URL opened and leaked a connection")
	}
	if networkError, ok := err.(net.Error); !ok || !networkError.Timeout() {
		test.Fatalf("unexpected accept failure: %v", err)
	}
}
