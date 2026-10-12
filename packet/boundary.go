package packet

func IsVideoBoundary(frame *Packet) bool {
	if frame.Type != PktVideo {
		return false
	}
	header, ok := frame.Header.(VideoPacketHeader)
	return ok && header.IsKeyFrame() && !header.IsSeqHdr() && !header.IsEndOfSeq()
}

type SequenceHeaders struct {
	video *Packet
	audio *Packet
}

func (headers *SequenceHeaders) Observe(frame *Packet) {
	switch frame.Type {
	case PktVideo:
		if header, ok := frame.Header.(VideoPacketHeader); ok && header.IsSeqHdr() {
			copy := *frame
			copy.Data = append([]byte(nil), frame.Data...)
			copy.Media = append([]byte(nil), frame.Media...)
			headers.video = &copy
		}
	case PktAudio:
		if header, ok := frame.Header.(AudioPacketHeader); ok && header.IsAACSeqHdr() {
			copy := *frame
			copy.Data = append([]byte(nil), frame.Data...)
			copy.Media = append([]byte(nil), frame.Media...)
			headers.audio = &copy
		}
	}
}

func (headers *SequenceHeaders) Replay(writer Writer) error {
	for _, frame := range []*Packet{headers.video, headers.audio} {
		if frame != nil {
			copy := *frame
			copy.Baseline = 0
			copy.TimeStamp = 0
			if err := writer.Write(&copy); err != nil {
				return err
			}
		}
	}
	return nil
}
