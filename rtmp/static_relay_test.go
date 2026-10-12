package rtmp

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/rtmp/core"
)

type staticPeer struct {
	listener                         net.Listener
	accepted                         chan struct{}
	ready                            chan struct{}
	closed                           chan struct{}
	peers                            sync.Map
	workers                          sync.WaitGroup
	acceptOnce, readyOnce, closeOnce sync.Once
}

func staticRelayPeer(test *testing.T, stall bool) *staticPeer {
	test.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	peer := &staticPeer{listener: listener, accepted: make(chan struct{}), ready: make(chan struct{}), closed: make(chan struct{})}
	peer.workers.Add(1)
	go func() {
		defer peer.workers.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			peer.peers.Store(connection, true)
			peer.workers.Add(1)
			go func() {
				defer peer.workers.Done()
				defer peer.peers.Delete(connection)
				defer connection.Close()
				defer peer.closeOnce.Do(func() { close(peer.closed) })
				peer.acceptOnce.Do(func() { close(peer.accepted) })
				_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
				if stall {
					_, _ = io.Copy(io.Discard, connection)
					return
				}
				protocol := core.NewConn(connection, core.ConnBufSize)
				server := core.NewConnServer(protocol, 4096)
				if err := protocol.HandshakeServer(); err != nil {
					return
				}
				if err := server.CommandLinkup(); err != nil {
					return
				}
				if err := server.Flush(); err != nil {
					return
				}
				peer.readyOnce.Do(func() { close(peer.ready) })
				for {
					if err := server.Read(&core.ChunkStream{}); err != nil {
						return
					}
				}
			}()
		}
	}()
	test.Cleanup(func() {
		_ = listener.Close()
		peer.peers.Range(func(connection, _ any) bool { _ = connection.(net.Conn).Close(); return true })
		peer.workers.Wait()
	})
	return peer
}

func staticRelayConfiguration(test *testing.T, source string, destinations ...string) {
	test.Helper()
	settings := config.NewConfig()
	settings.RTMP.Addr, settings.RTMP.DialTimeout = source, 3
	settings.App = []config.App{{Enable: true, AppName: "source", StaticPush: destinations}}
	encoded, err := json.Marshal(settings)
	if err != nil {
		test.Fatal(err)
	}
	filename := filepath.Join(test.TempDir(), "config.json")
	if err := os.WriteFile(filename, encoded, 0600); err != nil {
		test.Fatal(err)
	}
	if _, err := config.NewManager(context.Background(), filename); err != nil {
		test.Fatal(err)
	}
}

func awaitStaticPeer(test *testing.T, ready <-chan struct{}, label string, timeout time.Duration) {
	test.Helper()
	select {
	case <-ready:
	case <-time.After(timeout):
		test.Fatal(label)
	}
}

func startStaticRelayForTest(test *testing.T, destinations ...string) (*Stream, <-chan struct{}) {
	test.Helper()
	source := staticRelayPeer(test, false)
	staticRelayConfiguration(test, source.listener.Addr().String(), destinations...)
	stream := NewStream(base.NewInfo("source", "sample", false))
	finished := make(chan struct{})
	go func() { stream.staticRelay(); close(finished) }()
	test.Cleanup(func() {
		stream.inside.control.Cancel()
		awaitStaticPeer(test, finished, "static supervisor did not exit", 4*time.Second)
	})
	return stream, finished
}

func TestStaticRelayHealthyTargetDoesNotWaitForStalledSibling(test *testing.T) {
	lifecycleConfiguration(test)
	stalled, healthy := staticRelayPeer(test, true), staticRelayPeer(test, false)
	startStaticRelayForTest(test, "rtmp://"+stalled.listener.Addr().String()+"/stalled", "rtmp://"+healthy.listener.Addr().String()+"/healthy")
	awaitStaticPeer(test, stalled.accepted, "stalled target not attempted", time.Second)
	awaitStaticPeer(test, healthy.ready, "healthy target waited for stalled sibling's three-second startup budget", 500*time.Millisecond)
}

func TestStaticRelayCancellationInterruptsPendingHandshake(test *testing.T) {
	lifecycleConfiguration(test)
	stalled := staticRelayPeer(test, true)
	stream, finished := startStaticRelayForTest(test, "rtmp://"+stalled.listener.Addr().String()+"/stalled")
	awaitStaticPeer(test, stalled.accepted, "stalled target not attempted", time.Second)
	stream.inside.control.Cancel()
	awaitStaticPeer(test, finished, "static startup ignored publication cancellation", 500*time.Millisecond)
	awaitStaticPeer(test, stalled.closed, "cancelled static target socket remained open", 500*time.Millisecond)
}

func TestConnCloseJoinsStaticRelayStartupCancellation(test *testing.T) {
	connection, listener := lifecycleListener(test)
	stalled := staticRelayPeer(test, true)
	staticRelayConfiguration(test, listener.Addr().String(), "rtmp://"+stalled.listener.Addr().String()+"/stalled")
	reader := &takeoverReader{Universal: core.NewUniversal(base.NewInfo("source", "sample", false)), done: make(chan struct{})}
	connection.InstallReader(reader)
	awaitStaticPeer(test, stalled.accepted, "stalled target not attempted", time.Second)
	started := time.Now()
	connection.Close()
	if time.Since(started) > 500*time.Millisecond {
		test.Fatal("owner shutdown waited for static startup timeout")
	}
	awaitStaticPeer(test, stalled.closed, "Conn.Close left static target startup alive", 500*time.Millisecond)
}
