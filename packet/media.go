package packet

import "fmt"

// MediaKind describes the role of a demuxed packet in AVC/AAC output.
type MediaKind uint8

const (
	MediaOther MediaKind = iota
	MediaIgnored
	MediaVideo
	MediaAudio
	MediaAVCHeader
	MediaAACHeader
)

// MediaInfo contains only packet facts; output-specific caching and timing stay
// with the consumer. CompositionTime retains the signed AVC offset.
type MediaInfo struct {
	Kind            MediaKind
	Keyframe        bool
	CompositionTime int32
}

// ClassifyMedia checks codec support and distinguishes sequence headers from
// media. The packet must already have a demuxed audio/video header. Sequence
// header payload validation remains the responsibility of the destination muxer.
func ClassifyMedia(p *Packet) (MediaInfo, error) {
	switch p.Type {
	case PktVideo:
		h := p.Header.(VideoPacketHeader)
		if !h.IsCodecAvc() {
			return MediaInfo{}, fmt.Errorf("support H264 only,code id=%d", h.CodecID())
		}
		if h.IsSeqHdr() {
			return MediaInfo{Kind: MediaAVCHeader}, nil
		}
		if h.IsEndOfSeq() {
			return MediaInfo{Kind: MediaIgnored}, nil
		}
		return MediaInfo{Kind: MediaVideo, Keyframe: h.IsKeyFrame(), CompositionTime: h.CompositionTime()}, nil
	case PktAudio:
		h := p.Header.(AudioPacketHeader)
		if !h.IsSoundAAC() {
			return MediaInfo{}, fmt.Errorf("support AAC only,sound format=%d", h.SoundFormat())
		}
		if h.IsAACSeqHdr() {
			return MediaInfo{Kind: MediaAACHeader}, nil
		}
		return MediaInfo{Kind: MediaAudio}, nil
	case PktMetadata:
		return MediaInfo{Kind: MediaIgnored}, nil
	default:
		return MediaInfo{Kind: MediaOther}, nil
	}
}
