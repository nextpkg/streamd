package ts

import (
	"bytes"
	"testing"

	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func resumeFixture(test *testing.T) []byte {
	test.Helper()
	var output bytes.Buffer
	mixer := NewMixer(&output)
	require.NoError(test, mixer.SetTsHeader())
	for _, sample := range []struct {
		kind int
		dts  int64
		pts  int64
	}{{packet.PktVideo, 40, 160}, {packet.PktAudio, 67, 67}, {packet.PktVideo, 80, 120}, {packet.PktAudio, 90, 90}} {
		frame := &packet.Packet{Type: sample.kind, Data: []byte{0x27, 1, 0, 0, 0}, Media: []byte{1, 2, 3}}
		if sample.kind == packet.PktAudio {
			frame.Data = []byte{0xaf, 1}
		}
		require.NoError(test, flv.NewDemuxer().Demux(frame))
		require.NoError(test, mixer.muxer.Mux(frame, sample.dts*90, sample.pts*90, &output))
	}
	return output.Bytes()
}

func TestResumeTSRestoresTimeAndSignalsCounterReset(test *testing.T) {
	existing := resumeFixture(test)
	var output bytes.Buffer
	mixer := NewMixer(&output)
	offset, err := mixer.Resume(bytes.NewReader(existing), int64(len(existing)))
	require.NoError(test, err)
	require.Equal(test, uint32(200), offset)
	require.Equal(test, 8*tsPacketLen, output.Len())
	for index, expectedPID := range []uint16{0x11, 0, 0x1001, videoPID, audioPID} {
		packet := output.Bytes()[index*tsPacketLen : (index+1)*tsPacketLen]
		require.Equal(test, byte(0x47), packet[0])
		require.Equal(test, expectedPID, uint16(packet[1]&0x1f)<<8|uint16(packet[2]))
		require.Equal(test, byte(0x20), packet[3]&0x30)
		require.Equal(test, byte(183), packet[4])
		require.Equal(test, byte(0x80), packet[5])
	}
}

func TestResumeRejectsDamagedTSWithoutWriting(test *testing.T) {
	existing := resumeFixture(test)
	corrupt := bytes.Clone(existing)
	corrupt[len(corrupt)-tsPacketLen] = 0
	for _, damaged := range [][]byte{existing[:len(existing)-1], corrupt, bytes.Repeat([]byte{0}, tsPacketLen)} {
		var output bytes.Buffer
		_, err := NewMixer(&output).Resume(bytes.NewReader(damaged), int64(len(damaged)))
		require.Error(test, err)
		require.Zero(test, output.Len())
	}
}
