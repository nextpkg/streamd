package packet_test

import (
	"testing"

	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

type headerWriter struct{ frames []packet.Packet }

func (writer *headerWriter) Write(frame *packet.Packet) error {
	writer.frames = append(writer.frames, *frame)
	frame.Baseline = 500
	return nil
}

func TestSequenceHeadersOwnCachedPayload(t *testing.T) {
	headers := &packet.SequenceHeaders{}
	video := &packet.Packet{Type: packet.PktVideo, Baseline: 1000, TimeStamp: 40,
		Data: []byte{0x17, 0, 0, 0, 0, 1, 2, 3}}
	audio := &packet.Packet{Type: packet.PktAudio, Data: []byte{0xaf, 0, 0x12, 0x10}}
	require.NoError(t, flv.NewDemuxer().Demux(video))
	require.NoError(t, flv.NewDemuxer().Demux(audio))
	headers.Observe(video)
	headers.Observe(audio)
	video.Data[5], audio.Data[2] = 255, 255
	writer := &headerWriter{}
	require.NoError(t, headers.Replay(writer))
	require.NoError(t, headers.Replay(writer))
	require.Len(t, writer.frames, 4)
	for _, index := range []int{0, 2} {
		require.Equal(t, byte(1), writer.frames[index].Data[5])
		require.Equal(t, byte(1), writer.frames[index].Media[0])
		require.Zero(t, writer.frames[index].Baseline)
		require.Zero(t, writer.frames[index].TimeStamp)
	}
	require.Equal(t, byte(0x12), writer.frames[1].Data[2])
	require.Equal(t, byte(0x12), writer.frames[3].Media[0])
}

func TestVideoBoundaryExcludesHeadersAndOtherFrames(t *testing.T) {
	for _, test := range []struct {
		data []byte
		want bool
	}{
		{[]byte{0x17, 0, 0, 0, 0}, false},
		{[]byte{0x17, 1, 0, 0, 0}, true},
		{[]byte{0x17, 2, 0, 0, 0}, false},
		{[]byte{0x27, 1, 0, 0, 0}, false},
	} {
		frame := &packet.Packet{Type: packet.PktVideo, Data: test.data}
		require.NoError(t, flv.NewDemuxer().Demux(frame))
		require.Equal(t, test.want, packet.IsVideoBoundary(frame))
	}
	require.False(t, packet.IsVideoBoundary(&packet.Packet{Type: packet.PktAudio}))
	require.False(t, packet.IsVideoBoundary(&packet.Packet{Type: packet.PktVideo}))
}
