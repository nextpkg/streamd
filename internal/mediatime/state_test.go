package mediatime

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForwardTimelineMatchesReverseTail(test *testing.T) {
	samples := []struct {
		kind                   int
		decoding, presentation int64
	}{{0, 0, 80}, {0, 40, 160}, {1, 67, 67}, {0, 80, 120}, {1, 90, 90}}
	var forward, reverse Tail
	for _, sample := range samples {
		forward.ObserveForward(sample.kind, sample.decoding, sample.presentation)
	}
	for index := len(samples) - 1; index >= 0; index-- {
		sample := samples[index]
		reverse.ObserveReverse(sample.kind, sample.decoding, sample.presentation)
	}
	require.Equal(test, uint64(200), forward.NextValue())
	require.Equal(test, reverse.NextValue(), forward.NextValue())
	forward.ObserveForward(0, math.MaxUint32, math.MaxUint32)
	require.Greater(test, forward.NextValue(), uint64(math.MaxUint32))
}

func TestMediaStateRequiresBoundedTimeAndMatchingExtent(test *testing.T) {
	valid := &State{Format: "flv", Bytes: 1024, Next: 200}
	require.NoError(test, valid.Validate(1024))
	copy := valid.Clone()
	copy.Next = 300
	require.Equal(test, uint64(200), valid.Next)
	for _, invalid := range []*State{nil, {}, {Format: "mp4", Bytes: 1024, Next: 200}, {Format: "flv", Bytes: 1023, Next: 200}, {Format: "flv", Bytes: 1024}, {Format: "ts", Bytes: 1024, Next: math.MaxUint32 + 1}} {
		require.Error(test, invalid.Validate(1024))
	}
}
