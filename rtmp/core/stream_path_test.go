package core

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/nextpkg/goav/amf"
)

func TestMalformedStreamCommandsRejectBeforeResponse(test *testing.T) {
	for _, method := range []string{Publish, Play} {
		for _, arguments := range [][]interface{}{
			{method, float64(0), nil},
			{method, float64(0), nil, float64(1)},
			{method, float64(0), nil, "../other/sample"},
			{method, float64(0), nil, "", "live"},
		} {
			server := &ConnServer{}
			var err error
			if method == Publish {
				err = server.publishMsg(&ChunkStream{}, arguments)
			} else {
				err = server.playMsg(&ChunkStream{}, arguments)
			}
			if err == nil || server.done {
				test.Fatalf("malformed %s accepted: %#v", method, arguments)
			}
		}
	}
	for _, properties := range []interface{}{nil, "live", amf.Object{}, amf.Object{"app": float64(1)}, amf.Object{"app": ".."}} {
		server := &ConnServer{}
		if err := server.connectMsg(&ChunkStream{}, []interface{}{Connect, float64(1), properties}); err == nil {
			test.Fatalf("malformed connect accepted: %#v", properties)
		}
	}
}

func TestRTMPPreservesLiteralStreamNames(test *testing.T) {
	for _, name := range []string{"sample", "camera/front", "摄像头/大厅", "%2e%2e/sample", "mp4:clip", "camera one"} {
		for _, method := range []string{Publish, Play} {
			test.Run(method+"/"+name, func(test *testing.T) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					test.Fatal(err)
				}
				defer listener.Close()
				completed := make(chan error, 1)
				go func() {
					transport, err := listener.Accept()
					if err != nil {
						completed <- err
						return
					}
					defer transport.Close()
					_ = transport.SetDeadline(time.Now().Add(2 * time.Second))
					protocol := NewConn(transport, ConnBufSize)
					if err := protocol.HandshakeServer(); err != nil {
						completed <- err
						return
					}
					server := NewConnServer(protocol, 128)
					err = server.CommandLinkup()
					if err == nil {
						app, instance := server.GetInfo()
						if app != "直播" || instance != name || server.Method() != method {
							test.Errorf("wire identity changed: %q/%q %s", app, instance, server.Method())
						}
					}
					completed <- err
				}()
				transport, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
				if err != nil {
					_ = listener.Close()
					<-completed
					test.Fatal(err)
				}
				defer transport.Close()
				_ = transport.SetDeadline(time.Now().Add(2 * time.Second))
				client := NewConnClient("直播", name, NewConn(transport, ConnBufSize))
				if err := client.Start(method); err != nil {
					test.Error(err)
				}
				if err := <-completed; err != nil {
					test.Fatal(err)
				}
			})
		}
	}
}

func TestPlayNameComesFromStreamArgument(test *testing.T) {
	serverSide, clientSide := net.Pipe()
	defer serverSide.Close()
	defer clientSide.Close()
	_ = serverSide.SetDeadline(time.Now().Add(time.Second))
	done := make(chan struct{})
	go func() { defer close(done); _, _ = io.Copy(io.Discard, clientSide) }()
	defer func() { _ = serverSide.Close(); <-done }()
	server := NewConnServer(NewConn(serverSide, ConnBufSize), 128)
	if err := server.playMsg(&ChunkStream{csid: 3, streamID: 1}, []interface{}{Play, float64(0), nil, "camera/front", "../other/sample"}); err != nil {
		test.Fatal(err)
	}
	if server.publish.Name != "camera/front" {
		test.Fatalf("optional argument replaced stream name: %q", server.publish.Name)
	}
}
