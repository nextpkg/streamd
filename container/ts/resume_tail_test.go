package ts

import (
	"bytes"
	"testing"

	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
)

func longResumePES(test *testing.T, kind int, size int) []byte {
	test.Helper()
	var output bytes.Buffer
	mixer := NewMixer(&output)
	if err := mixer.SetTsHeader(); err != nil {
		test.Fatal(err)
	}
	frame := &packet.Packet{Type: kind, Data: []byte{0x27, 1, 0, 0, 0}}
	if kind == packet.PktAudio {
		frame.Data = []byte{0xaf, 1}
	}
	frame.Data = append(frame.Data, bytes.Repeat([]byte{0x31}, size)...)
	if err := flv.NewDemuxer().Demux(frame); err != nil {
		test.Fatal(err)
	}
	if err := mixer.muxer.Mux(frame, 40*90, 80*90, &output); err != nil {
		test.Fatal(err)
	}
	if output.Len() < 3*tsPacketLen+size {
		test.Fatal("fixture does not contain the requested PES payload")
	}
	return output.Bytes()
}

func TestResumeRejectsPacketAlignedIncompletePES(test *testing.T) {
	for _, kind := range []int{packet.PktVideo, packet.PktAudio} {
		existing := longResumePES(test, kind, 1024)
		for removed := tsPacketLen; removed <= 3*tsPacketLen; removed += tsPacketLen {
			var output bytes.Buffer
			damaged := existing[:len(existing)-removed]
			_, err := NewMixer(&output).Resume(bytes.NewReader(damaged), int64(len(damaged)))
			if err == nil || output.Len() != 0 {
				test.Errorf("packet-aligned torn PES accepted: kind=%d removed=%d output=%d error=%v", kind, removed, output.Len(), err)
			}
		}
	}
}

func TestResumeAcceptsCompleteMultiPacketPES(test *testing.T) {
	for _, kind := range []int{packet.PktVideo, packet.PktAudio} {
		for _, size := range []int{170, 1024, 65500, 256 * 1024} {
			existing := longResumePES(test, kind, size)
			var output bytes.Buffer
			offset, err := NewMixer(&output).Resume(bytes.NewReader(existing), int64(len(existing)))
			if err != nil || offset != 81 || output.Len() == 0 {
				test.Errorf("complete multi-packet PES rejected: kind=%d size=%d offset=%d error=%v", kind, size, offset, err)
			}
		}
	}
}

func TestResumeValidatesIndependentPESPayloadBoundaries(test *testing.T) {
	video := longResumePES(test, packet.PktVideo, 1024)
	audio := longResumePES(test, packet.PktAudio, 1024)
	interleaved := bytes.Clone(video[:3*tsPacketLen])
	for index := 3 * tsPacketLen; index < len(video); index += tsPacketLen {
		interleaved = append(interleaved, video[index:index+tsPacketLen]...)
		interleaved = append(interleaved, audio[index:index+tsPacketLen]...)
	}
	var valid bytes.Buffer
	if _, err := NewMixer(&valid).Resume(bytes.NewReader(interleaved), int64(len(interleaved))); err != nil {
		test.Fatal("complete interleaved PES rejected:", err)
	}
	badAdaptation := bytes.Clone(video)
	badAdaptation[len(video)-tsPacketLen+4] = 184
	missingMiddle := append(bytes.Clone(video[:4*tsPacketLen]), video[5*tsPacketLen:]...)
	duplicate := append(bytes.Clone(video), video[len(video)-tsPacketLen:]...)
	missingStart := append(bytes.Clone(video[:3*tsPacketLen]), video[4*tsPacketLen:]...)
	brokenVideoHealthyAudio := append(bytes.Clone(video[:len(video)-tsPacketLen]), audio[3*tsPacketLen:]...)
	for _, data := range [][]byte{badAdaptation, missingMiddle, duplicate, missingStart, brokenVideoHealthyAudio} {
		var output bytes.Buffer
		if _, err := NewMixer(&output).Resume(bytes.NewReader(data), int64(len(data))); err == nil || output.Len() != 0 {
			test.Fatalf("damaged payload accepted: bytes=%d output=%d error=%v", len(data), output.Len(), err)
		}
	}
}
