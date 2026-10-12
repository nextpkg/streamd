package gop

import (
	"testing"

	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

type packetCollector struct{ packets []*packet.Packet }

func (collector *packetCollector) Write(frame *packet.Packet) error {
	collector.packets = append(collector.packets, frame)
	return nil
}

func cachedFrame(test *testing.T, kind int, data []byte, timestamp uint32) *packet.Packet {
	test.Helper()
	frame := &packet.Packet{Type: kind, Data: data, TimeStamp: timestamp}
	require.NoError(test, flv.NewDemuxer().Demux(frame))
	return frame
}

func TestGOPReplaysAudioInMediaOrder(test *testing.T) {
	cache := NewCache(1)
	videoHeader := cachedFrame(test, packet.PktVideo, []byte{0x17, 0, 0, 0, 0}, 0)
	audioHeader := cachedFrame(test, packet.PktAudio, []byte{0xaf, 0, 0x12, 0x10}, 0)
	preroll := cachedFrame(test, packet.PktAudio, []byte{0xaf, 1, 1}, 0)
	keyframe := cachedFrame(test, packet.PktVideo, []byte{0x17, 1, 0, 0, 0}, 23)
	firstAudio := cachedFrame(test, packet.PktAudio, []byte{0xaf, 1, 2}, 23)
	secondAudio := cachedFrame(test, packet.PktAudio, []byte{0xaf, 1, 3}, 46)
	video := cachedFrame(test, packet.PktVideo, []byte{0x27, 1, 0, 0, 0}, 63)
	for _, frame := range []*packet.Packet{videoHeader, audioHeader, preroll, keyframe, firstAudio, secondAudio, video} {
		require.NoError(test, cache.Write(frame))
	}
	var collector packetCollector
	require.NoError(test, cache.SendTo(&collector))
	require.Equal(test, []*packet.Packet{videoHeader, audioHeader, keyframe, firstAudio, secondAudio, video}, collector.packets)
	newKey := cachedFrame(test, packet.PktVideo, []byte{0x17, 1, 0, 0, 0}, 2023)
	newAudio := cachedFrame(test, packet.PktAudio, []byte{0xaf, 1, 4}, 2030)
	require.NoError(test, cache.Write(newKey))
	require.NoError(test, cache.Write(newAudio))
	collector.packets = nil
	require.NoError(test, cache.SendTo(&collector))
	require.Equal(test, []*packet.Packet{videoHeader, audioHeader, newKey, newAudio}, collector.packets)
}
