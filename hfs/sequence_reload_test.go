package hfs

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func TestHTTPFLVForwardsReplacementSequenceHeaders(t *testing.T) {
	response := httptest.NewRecorder()
	funnel := NewStream("live", t.Name(), response)
	stream := funnel.Terminal.(*Stream)
	defer func() { funnel.Close(); funnel.Wait() }()
	video := &packet.Packet{Type: packet.PktVideo, Data: []byte{0x17, 0, 0, 0, 0, 1, 0x64, 0, 0x1f}}
	audio := &packet.Packet{Type: packet.PktAudio, Data: []byte{0xaf, 0, 0x12, 0x10}}
	keyframe := &packet.Packet{Type: packet.PktVideo, Data: []byte{0x17, 1, 0, 0, 0, 0, 0, 0, 1, 0x65}}
	for _, frame := range []*packet.Packet{video, audio, keyframe} {
		require.NoError(t, flv.NewDemuxer().Demux(frame))
		require.NoError(t, stream.Write(frame))
	}
	initial := response.Body.Len()
	video.Baseline, audio.Baseline = 1000, 1000
	require.NoError(t, stream.Write(video))
	require.NoError(t, stream.Write(audio))
	require.Equal(t, len(video.Data)+len(audio.Data)+30, response.Body.Len()-initial)
	require.Equal(t, 1, bytes.Count(response.Body.Bytes(), []byte("FLV")), "replacement headers must not restart the FLV container")
}
