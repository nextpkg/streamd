package gop

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
)

func startupCache(test *testing.T) (*Cache, []*packet.Packet) {
	test.Helper()
	cache := NewCache(1)
	frames := []*packet.Packet{
		{Type: packet.PktMetadata, Data: []byte{1}},
		{Type: packet.PktVideo, Data: []byte{0x17, 0, 0, 0, 0, 1}},
		{Type: packet.PktAudio, Data: []byte{0xaf, 0, 0x12, 0x10}},
		{Type: packet.PktVideo, Data: []byte{0x17, 1, 0, 0, 0, 2}},
		{Type: packet.PktAudio, Data: []byte{0xaf, 1, 3}},
		{Type: packet.PktVideo, Data: []byte{0x27, 1, 0, 0, 0, 4}},
	}
	for _, frame := range frames {
		if err := flv.NewDemuxer().Demux(frame); err != nil {
			test.Fatal(err)
		}
		if err := cache.Write(frame); err != nil {
			test.Fatal(err)
		}
	}
	return cache, frames
}

func TestGOPStartupOrderRetainsSharedPayloads(test *testing.T) {
	cache, frames := startupCache(test)
	first, second := &packetCollector{}, &packetCollector{}
	if err := cache.SendTo(first); err != nil {
		test.Fatal(err)
	}
	if err := cache.SendTo(second); err != nil {
		test.Fatal(err)
	}
	if len(first.packets) != len(frames) || len(second.packets) != len(frames) {
		test.Fatal("GOP startup was incomplete")
	}
	for index, original := range frames {
		if first.packets[index] != original || second.packets[index] != original || &first.packets[index].Data[0] != &original.Data[0] {
			test.Fatalf("startup order or shared payload changed at %d", index)
		}
	}
}

func TestGOPRingReusesStorageAndRetiresOldGroups(test *testing.T) {
	cache := newGopCache(2)
	first, second, third := &packet.Packet{TimeStamp: 1}, &packet.Packet{TimeStamp: 2}, &packet.Packet{TimeStamp: 3}
	if err := cache.Write(first, true); err != nil {
		test.Fatal(err)
	}
	storage := cache.gops[0]
	if err := cache.Write(second, true); err != nil {
		test.Fatal(err)
	}
	if err := cache.Write(third, true); err != nil {
		test.Fatal(err)
	}
	collector := &packetCollector{}
	if err := cache.SendTo(collector); err != nil {
		test.Fatal(err)
	}
	if cache.gops[0] != storage || len(collector.packets) != 2 || collector.packets[0] != second || collector.packets[1] != third {
		test.Fatal("GOP ring allocated replacement storage or retained the wrong groups")
	}
}

// A failed viewer must stop receiving immediately without consuming the cache
// that a subsequent viewer needs to start playback.
type replayWriter func(*packet.Packet) error

func (write replayWriter) Write(p *packet.Packet) error { return write(p) }

func TestGOPReplayStopsOnViewerFailure(t *testing.T) {
	for index, name := range []string{"metadata", "video-header", "audio-header", "keyframe", "audio", "inter-frame"} {
		t.Run(name, func(t *testing.T) {
			cache, frames := startupCache(t)
			failure := errors.New("viewer disconnected")
			var received []*packet.Packet
			err := cache.SendTo(replayWriter(func(p *packet.Packet) error {
				received = append(received, p)
				if len(received) == index+1 {
					return failure
				}
				return nil
			}))
			require.ErrorIs(t, err, failure)
			require.Equal(t, frames[:index+1], received)
			next := &packetCollector{}
			require.NoError(t, cache.SendTo(next))
			require.Equal(t, frames, next.packets)
		})
	}
}
