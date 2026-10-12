package rtmp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/rtmp/core"
)

func lifecycleConfiguration(test *testing.T) {
	test.Helper()
	previous, err := json.Marshal(config.Get())
	if err != nil {
		test.Fatal(err)
	}
	filename := filepath.Join(test.TempDir(), "service.json")
	if err := os.WriteFile(filename, []byte(`{"rtmp":{"addr":"127.0.0.1:0","read_timeout":1}}`), 0600); err != nil {
		test.Fatal(err)
	}
	if _, err := config.NewManager(context.Background(), filename); err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() {
		if err := os.WriteFile(filename, previous, 0600); err != nil {
			test.Error(err)
			return
		}
		if _, err := config.NewManager(context.Background(), filename); err != nil {
			test.Error(err)
		}
	})
}

func lifecycleListener(test *testing.T) (*Conn, net.Listener) {
	test.Helper()
	lifecycleConfiguration(test)
	connection := NewConn(context.Background())
	listener, err := connection.StartRTMP()
	if err != nil {
		connection.Close()
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = listener.Close(); connection.Close() })
	return connection, listener
}

func lifecyclePeer(test *testing.T, listener net.Listener, handshake bool) net.Conn {
	test.Helper()
	transport, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = transport.Close() })
	if handshake {
		protocol := core.NewConn(transport, core.ConnBufSize)
		if err := protocol.HandshakeClient(); err != nil {
			test.Fatal(err)
		}
	}
	return transport
}

func TestConnCloseRetiresPendingRTMPNegotiation(test *testing.T) {
	connection, listener := lifecycleListener(test)
	peer := lifecyclePeer(test, listener, true)
	_ = listener.Close()
	connection.Close()
	_ = peer.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	var packet [1]byte
	_, err := peer.Read(packet[:])
	if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		test.Fatalf("Conn.Close left a negotiated half connection alive: %v", err)
	}
}

func TestRTMPCommandNegotiationHasAbsoluteTimeout(test *testing.T) {
	_, listener := lifecycleListener(test)
	peer := lifecyclePeer(test, listener, true)
	started := time.Now()
	if _, err := peer.Write([]byte{3, 0, 0, 0, 0, 1, 0, 20, 0, 0, 0, 0}); err != nil {
		test.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if _, err := peer.Write([]byte{0}); err != nil {
					return
				}
			}
		}
	}()
	defer func() { close(stop); <-done }()
	_ = peer.SetReadDeadline(time.Now().Add(1800 * time.Millisecond))
	var packet [1]byte
	_, err := peer.Read(packet[:])
	if (!errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET)) || time.Since(started) < 800*time.Millisecond || time.Since(started) > 1600*time.Millisecond {
		test.Fatalf("trickled command escaped the negotiation budget: elapsed=%s error=%v", time.Since(started), err)
	}
}

func TestConnCloseOwnsListenerAndJoinsPendingHandlers(test *testing.T) {
	connection, listener := lifecycleListener(test)
	var peers []net.Conn
	for index := 0; index < 8; index++ {
		peers = append(peers, lifecyclePeer(test, listener, index%2 == 0))
	}
	server := connection.servers[0]
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		server.mutex.Lock()
		count := len(server.pending)
		server.mutex.Unlock()
		if count == len(peers) {
			break
		}
		if time.Now().After(deadline) {
			test.Fatalf("only %d peers were accepted", count)
		}
		time.Sleep(time.Millisecond)
	}
	var callers sync.WaitGroup
	for index := 0; index < 16; index++ {
		callers.Add(1)
		go func() { defer callers.Done(); connection.Close() }()
	}
	done := make(chan struct{})
	go func() { callers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		test.Fatal("concurrent close did not join accepted handlers promptly")
	}
	server.mutex.Lock()
	remaining := len(server.pending) + len(server.listeners)
	server.mutex.Unlock()
	if remaining != 0 {
		test.Fatalf("close returned with %d tracked network resources", remaining)
	}
	for _, peer := range peers {
		_ = peer.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		var response [1]byte
		if _, err := peer.Read(response[:]); !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
			test.Fatalf("half connection was not closed: %v", err)
		}
	}
	late, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond)
	if err == nil {
		_ = late.Close()
		test.Fatal("close left its listener accepting clients")
	}
	if lateListener, err := connection.StartRTMP(); !errors.Is(err, net.ErrClosed) {
		if lateListener != nil {
			_ = lateListener.Close()
		}
		test.Fatalf("closed service restarted its listener: %v", err)
	}
}

type shutdownReader struct {
	*core.Universal
	entered chan struct{}
	closed  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (reader *shutdownReader) Read(*packet.Packet) error {
	close(reader.entered)
	<-reader.release
	return io.EOF
}

func (reader *shutdownReader) Close() { reader.once.Do(func() { close(reader.closed) }) }

func (reader *shutdownReader) IsTimeout() bool { return false }

func (reader *shutdownReader) GetPublish() *base.PublishInfo {
	return &base.PublishInfo{Name: reader.Info().Instance}
}

func (reader *shutdownReader) GetConnect() *base.ConnectInfo {
	return &base.ConnectInfo{App: reader.Info().App}
}

func TestConnCloseWaitsForActiveDispatch(test *testing.T) {
	connection := NewConn(context.Background())
	reader := &shutdownReader{Universal: core.NewUniversal(base.NewInfo("live", "shutdown", false)), entered: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
	defer connection.Close()
	connection.InstallReader(reader)
	<-reader.entered
	done := make(chan struct{})
	go func() { connection.Close(); close(done) }()
	<-reader.closed
	select {
	case <-done:
		close(reader.release)
		test.Fatal("close returned before the publisher dispatch loop exited")
	case <-time.After(20 * time.Millisecond):
	}
	close(reader.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		test.Fatal("close did not join the released publisher")
	}
	if len(connection.PublicationKeys()) != 0 || len(connection.StreamKeys()) != 0 {
		test.Fatal("closed service retained publication statistics")
	}
}

func TestRTMPRejectedHandshakeReleasesPendingSlot(test *testing.T) {
	for _, mode := range []string{"empty", "partial", "invalid-version"} {
		test.Run(mode, func(test *testing.T) {
			connection, listener := lifecycleListener(test)
			peer := lifecyclePeer(test, listener, false)
			if mode == "partial" {
				_, _ = peer.Write([]byte{3, 0})
			}
			if mode == "invalid-version" {
				if _, err := peer.Write(make([]byte, 1537)); err != nil {
					test.Fatal(err)
				}
			}
			_ = peer.SetReadDeadline(time.Now().Add(3 * time.Second))
			var response [1]byte
			if _, err := peer.Read(response[:]); !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
				test.Fatalf("rejected handshake did not close: %v", err)
			}
			server := connection.servers[0]
			deadline := time.Now().Add(500 * time.Millisecond)
			for {
				server.mutex.Lock()
				count := len(server.pending)
				server.mutex.Unlock()
				if count == 0 {
					break
				}
				if time.Now().After(deadline) {
					test.Fatal("rejected handshake retained its pending slot")
				}
				time.Sleep(time.Millisecond)
			}
			connection.Close()
		})
	}
}

func TestConnImmediateCloseCannotLeaveListenerStarting(test *testing.T) {
	lifecycleConfiguration(test)
	for attempt := 0; attempt < 32; attempt++ {
		connection := NewConn(context.Background())
		listener, err := connection.StartRTMP()
		if err != nil {
			connection.Close()
			test.Fatal(err)
		}
		connection.Close()
		peer, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond)
		if err == nil {
			_ = peer.Close()
			test.Fatal("immediate close left a listener awaiting its accept goroutine")
		}
	}
}
