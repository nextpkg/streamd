package core

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func relayPeer(test *testing.T) (string, <-chan error) {
	test.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	finished := make(chan error, 4)
	var connections sync.Map
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Store(connection, true)
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer connections.Delete(connection)
				defer func() { _ = connection.Close() }()
				_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
				protocol := NewConn(connection, ConnBufSize)
				server := NewConnServer(protocol, 4096)
				err := protocol.HandshakeServer()
				if err == nil {
					err = server.CommandLinkup()
				}
				if err == nil {
					err = server.Flush()
				}
				if err == nil {
					err = server.Read(&ChunkStream{})
				}
				finished <- err
			}()
		}
	}()
	test.Cleanup(func() {
		_ = listener.Close()
		connections.Range(func(key, _ any) bool {
			_ = key.(net.Conn).Close()
			return true
		})
		workers.Wait()
	})
	return "rtmp://" + listener.Addr().String() + "/live/sample", finished
}

func TestDynamicRelayStopClosesIdlePeers(test *testing.T) {
	pullURL, pullFinished := relayPeer(test)
	pushURL, pushFinished := relayPeer(test)
	relay := NewDynamicRelay(pullURL, pushURL, time.Second)
	if relay == nil {
		test.Fatal("relay construction failed")
	}
	test.Cleanup(relay.Stop)
	if err := relay.Start(); err != nil {
		test.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	relay.Stop()
	for _, peer := range []struct {
		name string
		done <-chan error
	}{{"pull", pullFinished}, {"push", pushFinished}} {
		select {
		case err := <-peer.done:
			if !errors.Is(err, io.EOF) {
				test.Errorf("%s peer did not observe a clean close: %v", peer.name, err)
			}
		case <-time.After(500 * time.Millisecond):
			test.Errorf("%s peer remained connected after Stop", peer.name)
		}
	}
}
