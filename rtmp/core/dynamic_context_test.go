package core

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestDynamicRelayStartupContextCancelsEveryNetworkPhase(test *testing.T) {
	for _, phase := range []string{"dial-pull", "play", "dial-push", "publish"} {
		test.Run(phase, func(test *testing.T) {
			relay, puller, pusher := relayTestPair(test)
			relay.dialTimeout = time.Minute
			entered := make(chan struct{})
			original := relay.dial
			relay.dial = func(ctx context.Context, location string, timeout time.Duration) (relayClient, error) {
				if phase == "dial-"+location {
					close(entered)
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return original(ctx, location, timeout)
			}
			if phase == "play" {
				puller.play = func() error { close(entered); <-puller.closed; return net.ErrClosed }
			}
			if phase == "publish" {
				pusher.publish = func() error { close(entered); <-pusher.closed; return net.ErrClosed }
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- relay.StartContext(ctx) }()
			awaitRelaySignal(test, entered)
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					test.Fatalf("lost cancellation: %v", err)
				}
			case <-time.After(time.Second):
				test.Fatal("startup did not cancel")
			}
			awaitRelaySignal(test, relay.Wait())
		})
	}
}

func TestDynamicRelaySuccessOutlivesStartupRequest(test *testing.T) {
	relay, puller, pusher := relayTestPair(test)
	ctx, cancel := context.WithCancel(context.Background())
	if err := relay.StartContext(ctx); err != nil {
		cancel()
		test.Fatal(err)
	}
	cancel()
	select {
	case <-relay.Wait():
		test.Fatal("successful relay inherited request cancellation")
	case <-time.After(30 * time.Millisecond):
	}
	if puller.closeCalls.Load() != 0 || pusher.closeCalls.Load() != 0 {
		test.Fatal("request closed active relay clients")
	}
	if err := relay.Start(); err != nil {
		test.Fatal(err)
	}
	if puller.playCalls.Load() != 1 || pusher.publishCalls.Load() != 1 {
		test.Fatal("request cancellation caused a restart")
	}
}

func TestDynamicRelayQueuedStartupCancellationDoesNotStopOwner(test *testing.T) {
	relay, puller, _ := relayTestPair(test)
	relay.dialTimeout = time.Minute
	entered := make(chan struct{})
	puller.play = func() error { close(entered); <-puller.closed; return net.ErrClosed }
	owner := make(chan error, 1)
	go func() { owner <- relay.Start() }()
	defer func() { relay.Stop(); <-owner }()
	awaitRelaySignal(test, entered)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiter := make(chan error, 1)
	go func() { waiter <- relay.StartContext(ctx) }()
	cancel()
	select {
	case err := <-waiter:
		if !errors.Is(err, context.Canceled) {
			test.Fatalf("queued cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		test.Fatal("queued startup ignored cancellation")
	}
	if puller.closeCalls.Load() != 0 {
		test.Fatal("queued request cancelled another startup")
	}
}
