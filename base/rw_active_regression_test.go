package base

import (
	"testing"
	"time"

	"github.com/nextpkg/streamd/packet"
)

func TestReadTimeoutCanChangeWithoutReplacingSession(test *testing.T) {
	previous := readTimeout.Load()
	test.Cleanup(func() { readTimeout.Store(previous) })
	active := NewRwAlive()
	active.active.Store(time.Now().Add(-3 * time.Second).Unix())
	SetReadTimeout(10 * time.Second)
	if active.IsTimeout() {
		test.Fatal("session expired before configured timeout")
	}
	SetReadTimeout(time.Second)
	if !active.IsTimeout() {
		test.Fatal("existing session did not observe changed timeout")
	}
	SetReadTimeout(0)
	if active.IsTimeout() {
		test.Fatal("disabled timeout remained active")
	}
}

func TestRebaseUsesIndependentAtomicSnapshot(test *testing.T) {
	active := NewRwAlive()
	active.SetMediaTime(&packet.Packet{Type: packet.PktAudio, Baseline: 4000})
	active.SetMediaTime(&packet.Packet{Type: packet.PktVideo, Baseline: 3000})
	active.RebaseTime()
	active.SetMediaTime(&packet.Packet{Type: packet.PktAudio, Baseline: 5000})
	if active.GetBaseTime() != 4000 {
		test.Fatal("base timestamp changed without rebasing")
	}
}
