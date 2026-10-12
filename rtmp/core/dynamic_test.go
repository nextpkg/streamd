package core

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type relayTestClient struct {
	closed       chan struct{}
	closeOnce    sync.Once
	closeCalls   atomic.Int32
	playCalls    atomic.Int32
	publishCalls atomic.Int32
	play         func() error
	publish      func() error
	read         func(*ChunkStream) error
	write        func(*ChunkStream) error
}

func newRelayTestClient() *relayTestClient {
	return &relayTestClient{closed: make(chan struct{})}
}

func (client *relayTestClient) StartPlay() error {
	client.playCalls.Add(1)
	if client.play != nil {
		return client.play()
	}
	return nil
}

func (client *relayTestClient) StartPublish() error {
	client.publishCalls.Add(1)
	if client.publish != nil {
		return client.publish()
	}
	return nil
}

func (client *relayTestClient) Read(chunk *ChunkStream) error {
	if client.read != nil {
		return client.read(chunk)
	}
	<-client.closed
	return net.ErrClosed
}

func (client *relayTestClient) Write(chunk *ChunkStream) error {
	if client.write != nil {
		return client.write(chunk)
	}
	return nil
}

func (client *relayTestClient) Close() error {
	client.closeCalls.Add(1)
	client.closeOnce.Do(func() { close(client.closed) })
	return nil
}

func relayTestPair(test *testing.T) (*DynamicRelay, *relayTestClient, *relayTestClient) {
	test.Helper()
	relay := NewDynamicRelay("pull", "push", time.Second)
	puller, pusher := newRelayTestClient(), newRelayTestClient()
	relay.dial = func(_ context.Context, location string, _ time.Duration) (relayClient, error) {
		if location == "pull" {
			return puller, nil
		}
		return pusher, nil
	}
	test.Cleanup(func() {
		relay.Stop()
		awaitRelaySignal(test, relay.Wait())
	})
	return relay, puller, pusher
}

func awaitRelaySignal(test *testing.T, signal <-chan struct{}) {
	test.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		test.Fatal("relay lifecycle did not finish within one second")
	}
}

func TestDynamicRelayClosesStartupFailures(test *testing.T) {
	failure := errors.New("injected startup failure")
	for _, phase := range []string{"dial-pull", "dial-push", "play", "publish"} {
		test.Run(phase, func(test *testing.T) {
			relay, puller, pusher := relayTestPair(test)
			originalDial := relay.dial
			relay.dial = func(ctx context.Context, location string, timeout time.Duration) (relayClient, error) {
				if phase == "dial-"+location {
					return nil, failure
				}
				return originalDial(ctx, location, timeout)
			}
			if phase == "play" {
				puller.play = func() error { return failure }
			}
			if phase == "publish" {
				pusher.publish = func() error { return failure }
			}
			if err := relay.Start(); !errors.Is(err, failure) {
				test.Fatalf("startup failure was lost: %v", err)
			}
			awaitRelaySignal(test, relay.Wait())
			wantPull, wantPush := int32(1), int32(1)
			if phase == "dial-pull" {
				wantPull = 0
			}
			if phase == "dial-pull" || phase == "dial-push" || phase == "play" {
				wantPush = 0
			}
			if puller.closeCalls.Load() != wantPull || pusher.closeCalls.Load() != wantPush {
				test.Fatalf("clients leaked or closed twice: pull=%d push=%d", puller.closeCalls.Load(), pusher.closeCalls.Load())
			}
		})
	}
}

func TestDynamicRelayStopDuringStartup(test *testing.T) {
	for _, phase := range []string{"dial", "play", "publish"} {
		test.Run(phase, func(test *testing.T) {
			relay, puller, pusher := relayTestPair(test)
			entered := make(chan struct{})
			switch phase {
			case "dial":
				relay.dial = func(ctx context.Context, _ string, _ time.Duration) (relayClient, error) {
					close(entered)
					<-ctx.Done()
					return nil, ctx.Err()
				}
			case "play":
				puller.play = func() error { close(entered); <-puller.closed; return net.ErrClosed }
			case "publish":
				pusher.publish = func() error { close(entered); <-pusher.closed; return net.ErrClosed }
			}
			result := make(chan error, 1)
			go func() { result <- relay.Start() }()
			awaitRelaySignal(test, entered)
			relay.Stop()
			awaitRelaySignal(test, relay.Wait())
			if err := <-result; err == nil {
				test.Fatal("cancelled startup reported success")
			}
		})
	}
}

func TestDynamicRelayStartupDeadline(test *testing.T) {
	relay, puller, pusher := relayTestPair(test)
	relay.dialTimeout = 30 * time.Millisecond
	puller.play = func() error { <-puller.closed; return net.ErrClosed }
	started := time.Now()
	if err := relay.Start(); err == nil {
		test.Fatal("blocked startup reported success")
	}
	if time.Since(started) > time.Second || puller.closeCalls.Load() != 1 || pusher.closeCalls.Load() != 0 {
		test.Fatal("startup deadline did not close both clients promptly")
	}
}

func TestDynamicRelayQueueSaturationStopsBothPeers(test *testing.T) {
	relay, puller, pusher := relayTestPair(test)
	blocked := make(chan struct{})
	var reads int
	puller.read = func(chunk *ChunkStream) error {
		if reads == 1 {
			<-blocked
		}
		reads++
		chunk.timestamp = uint32(reads)
		return nil
	}
	pusher.write = func(*ChunkStream) error {
		close(blocked)
		<-pusher.closed
		return net.ErrClosed
	}
	if err := relay.Start(); err != nil {
		test.Fatal(err)
	}
	awaitRelaySignal(test, relay.Wait())
	if reads != maxQueue+2 || puller.closeCalls.Load() != 1 || pusher.closeCalls.Load() != 1 {
		test.Fatalf("queue saturation was not bounded: reads=%d, closes=%d/%d", reads, puller.closeCalls.Load(), pusher.closeCalls.Load())
	}
}

func TestDynamicRelayWaitJoinsWorkers(test *testing.T) {
	relay, puller, _ := relayTestPair(test)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	puller.read = func(*ChunkStream) error {
		close(entered)
		<-puller.closed
		<-release
		return net.ErrClosed
	}
	if err := relay.Start(); err != nil {
		test.Fatal(err)
	}
	awaitRelaySignal(test, entered)
	relay.Stop()
	select {
	case <-relay.Wait():
		test.Fatal("Wait completed before the read worker exited")
	default:
	}
	if relay.IsDone() {
		test.Fatal("IsDone reported worker completion too early")
	}
}

func TestDynamicRelayConcurrentLifecycle(test *testing.T) {
	relay := NewDynamicRelay("pull", "push", time.Second)
	var clients []*relayTestClient
	relay.dial = func(context.Context, string, time.Duration) (relayClient, error) {
		client := newRelayTestClient()
		clients = append(clients, client)
		return client, nil
	}
	for cycle := 0; cycle < 12; cycle++ {
		var workers sync.WaitGroup
		for caller := 0; caller < 12; caller++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				if err := relay.Start(); err != nil {
					test.Error(err)
				}
				_ = relay.IsDone()
				_ = relay.Wait()
			}()
		}
		workers.Wait()
		if len(clients) != 2*(cycle+1) {
			test.Fatalf("concurrent starts created %d clients", len(clients))
		}
		for caller := 0; caller < 12; caller++ {
			workers.Add(1)
			go func() { defer workers.Done(); relay.Stop() }()
		}
		workers.Wait()
		awaitRelaySignal(test, relay.Wait())
	}
	for _, client := range clients {
		if client.closeCalls.Load() != 1 || client.playCalls.Load()+client.publishCalls.Load() != 1 {
			test.Fatal("a retired client was leaked or reused")
		}
	}
}

func TestDynamicRelayReadWriteFailuresClosePair(test *testing.T) {
	for _, phase := range []string{"read", "write"} {
		test.Run(phase, func(test *testing.T) {
			relay, puller, pusher := relayTestPair(test)
			if phase == "read" {
				puller.read = func(*ChunkStream) error { return io.EOF }
			} else {
				var sent bool
				puller.read = func(*ChunkStream) error {
					if !sent {
						sent = true
						return nil
					}
					<-puller.closed
					return net.ErrClosed
				}
				pusher.write = func(*ChunkStream) error { return io.ErrClosedPipe }
			}
			if err := relay.Start(); err != nil {
				test.Fatal(err)
			}
			awaitRelaySignal(test, relay.Wait())
			if puller.closeCalls.Load() != 1 || pusher.closeCalls.Load() != 1 {
				test.Fatal("I/O failure did not close both clients")
			}
		})
	}
}

func TestDynamicRelayForwardsChunksAcrossRestart(test *testing.T) {
	relay := NewDynamicRelay("pull", "push", time.Second)
	for cycle := 0; cycle < 3; cycle++ {
		puller, pusher := newRelayTestClient(), newRelayTestClient()
		forwarded := make(chan ChunkStream, 1)
		var sent bool
		puller.read = func(chunk *ChunkStream) error {
			if sent {
				<-puller.closed
				return net.ErrClosed
			}
			sent = true
			*chunk = ChunkStream{timestamp: uint32(cycle + 20), typeID: 9, data: []byte{1, 2, 3}}
			return nil
		}
		pusher.write = func(chunk *ChunkStream) error { forwarded <- *chunk; return nil }
		relay.dial = func(_ context.Context, location string, _ time.Duration) (relayClient, error) {
			if location == "pull" {
				return puller, nil
			}
			return pusher, nil
		}
		if err := relay.Start(); err != nil {
			test.Fatal(err)
		}
		select {
		case chunk := <-forwarded:
			if chunk.timestamp != uint32(cycle+20) || chunk.typeID != 9 || string(chunk.data) != "\x01\x02\x03" {
				test.Fatalf("forwarded stale or corrupt chunk: %#v", chunk)
			}
		case <-time.After(time.Second):
			test.Fatal("relay did not forward media")
		}
		relay.RenewConnection()
		if !relay.IsDone() || puller.closeCalls.Load() != 1 || pusher.closeCalls.Load() != 1 {
			test.Fatal("renew did not join and close the previous generation")
		}
	}
}
