package core

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/packet"
)

func TestRTMPIdleDisconnectedViewerRetires(test *testing.T) {
	server, peer := net.Pipe()
	writer := NewWriter(&ConnClient{conn: NewConn(server, 4096)})
	test.Cleanup(func() { _ = peer.Close(); writer.Close(); writer.Wait() })
	done := make(chan struct{})
	go func() { writer.Wait(); close(done) }()
	if err := peer.Close(); err != nil {
		test.Fatal(err)
	}
	select {
	case <-done:
		if !writer.IsClosed() {
			test.Fatal("disconnected viewer still accepts packets")
		}
	case <-time.After(time.Second):
		test.Fatal("idle disconnected viewer retained its funnel until another media packet")
	}
}

type blockedWriteConnection struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (connection *blockedWriteConnection) Write(body []byte) (int, error) {
	connection.once.Do(func() { close(connection.entered) })
	return connection.Conn.Write(body)
}

func TestRTMPBlockedViewerAbortJoinsAndClearsQueue(test *testing.T) {
	for _, graceful := range []bool{false, true} {
		test.Run(map[bool]string{false: "abort", true: "bounded-drain"}[graceful], func(test *testing.T) {
			server, peer := net.Pipe()
			connection := &blockedWriteConnection{Conn: server, entered: make(chan struct{})}
			writer := NewWriter(&ConnClient{conn: NewConn(connection, 128)})
			writer.drainTimeout = 50 * time.Millisecond
			test.Cleanup(func() { _ = peer.Close(); writer.Abort(); writer.Wait() })
			frame := &packet.Packet{Type: packet.PktAudio, Data: make([]byte, 4096)}
			if err := writer.Write(frame); err != nil {
				test.Fatal(err)
			}
			select {
			case <-connection.entered:
			case <-time.After(time.Second):
				test.Fatal("network writer did not block")
			}
			for index := 0; index < maxQueueLen; index++ {
				if err := writer.Write(frame); err != nil {
					test.Fatal(err)
				}
			}
			if err := writer.Write(frame); err != ErrQueueSaturated {
				test.Fatalf("queue bound not enforced: %v", err)
			}
			if graceful {
				writer.Close()
			} else {
				writer.Abort()
			}
			done := make(chan struct{})
			go func() { writer.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				test.Fatal("blocked viewer did not join")
			}
			if len(writer.pktChan) != 0 {
				test.Fatal("retired queue retained media references")
			}
			if err := writer.Write(frame); err != ErrWriterWasCanceled {
				test.Fatalf("retired writer accepted media: %v", err)
			}
			select {
			case <-writer.Terminal.(*Writer).readDone:
			default:
				test.Fatal("control reader outlived writer retirement")
			}
		})
	}
}

type drainingTerminal struct {
	*Universal
	writes int
	aborts int
}

func (terminal *drainingTerminal) Name() string               { return "draining" }
func (terminal *drainingTerminal) Write(*packet.Packet) error { terminal.writes++; return nil }
func (terminal *drainingTerminal) Abort()                     { terminal.aborts++ }

func TestFunnelGracefulCloseDrainsHealthyTerminal(test *testing.T) {
	terminal := &drainingTerminal{Universal: NewUniversal(base.NewInfo("live", test.Name(), true))}
	writer := NewFunnel(terminal)
	writer.drainTimeout = time.Second
	for index := 0; index < 1024; index++ {
		if err := writer.Write(&packet.Packet{Type: packet.PktAudio, Data: []byte{1}}); err != nil {
			test.Fatal(err)
		}
	}
	writer.Close()
	writer.Wait()
	writer.Abort()
	if terminal.writes != 1024 || terminal.aborts != 0 || len(writer.pktChan) != 0 {
		test.Fatalf("graceful drain changed: writes=%d aborts=%d pending=%d", terminal.writes, terminal.aborts, len(writer.pktChan))
	}
}

func TestFunnelConcurrentAbortCloseAndWrite(test *testing.T) {
	server, peer := net.Pipe()
	writer := NewWriter(&ConnClient{conn: NewConn(server, 128)})
	test.Cleanup(func() { _ = peer.Close(); writer.Abort(); writer.Wait() })
	var workers sync.WaitGroup
	for index := 0; index < 32; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for repeat := 0; repeat < 16; repeat++ {
				_ = writer.Write(&packet.Packet{Type: packet.PktAudio, Data: make([]byte, 256)})
				writer.RebaseTime()
			}
			writer.Close()
			writer.Abort()
		}()
	}
	workers.Wait()
	writer.Wait()
	if !writer.IsClosed() || len(writer.pktChan) != 0 {
		test.Fatal("concurrent retirement left queued media")
	}
}
