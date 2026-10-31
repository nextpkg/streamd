package packet_test

import (
	"testing"

	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func TestClassifyMedia(t *testing.T) {
	for _, tt := range []struct {
		name    string
		typ     int
		data    []byte
		want    packet.MediaInfo
		failure string
	}{
		{"AVC configuration is not a keyframe", packet.PktVideo, []byte{0x17, 0, 0, 0, 0}, packet.MediaInfo{Kind: packet.MediaAVCHeader}, ""},
		{"AVC end is ignored", packet.PktVideo, []byte{0x17, 2, 0, 0, 0}, packet.MediaInfo{Kind: packet.MediaIgnored}, ""},
		{"keyframe with composition offset", packet.PktVideo, []byte{0x17, 1, 0, 1, 2}, packet.MediaInfo{Kind: packet.MediaVideo, Keyframe: true, CompositionTime: 258}, ""},
		{"inter frame", packet.PktVideo, []byte{0x27, 1, 0, 0, 3}, packet.MediaInfo{Kind: packet.MediaVideo, CompositionTime: 3}, ""},
		{"other video codec rejected before header classification", packet.PktVideo, []byte{0x12, 0, 0, 0, 0}, packet.MediaInfo{}, "support H264 only,code id=2"},
		{"AAC configuration", packet.PktAudio, []byte{0xaf, 0}, packet.MediaInfo{Kind: packet.MediaAACHeader}, ""},
		{"AAC media", packet.PktAudio, []byte{0xaf, 1}, packet.MediaInfo{Kind: packet.MediaAudio}, ""},
		{"MP3 rejected", packet.PktAudio, []byte{0x2f, 0}, packet.MediaInfo{}, "support AAC only,sound format=2"},
		{"source metadata ignored", packet.PktMetadata, nil, packet.MediaInfo{Kind: packet.MediaIgnored}, ""},
		{"other packet unchanged", 99, nil, packet.MediaInfo{Kind: packet.MediaOther}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &packet.Packet{Type: tt.typ, Data: tt.data, Baseline: 123, TimeStamp: 45}
			if p.Type == packet.PktVideo || p.Type == packet.PktAudio {
				require.NoError(t, flv.NewDemuxer().Demux(p))
			}
			before := *p
			got, err := packet.ClassifyMedia(p)
			if tt.failure != "" {
				require.EqualError(t, err, tt.failure)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
			require.Equal(t, before, *p, "classification must not rewrite packet timing or payload")
		})
	}
}
