package flv

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func resumeFixture(test *testing.T) []byte {
	test.Helper()
	var output bytes.Buffer
	mixer := NewMixer(&output)
	require.NoError(test, mixer.SetFlvHeader())
	require.NoError(test, mixer.Mux(&packet.Packet{Type: packet.PktVideo, Data: []byte{0x17, 1, 0, 0, 120, 1}}, 40))
	require.NoError(test, mixer.Mux(&packet.Packet{Type: packet.PktAudio, Data: []byte{0xaf, 1, 1}}, 67))
	require.NoError(test, mixer.Mux(&packet.Packet{Type: packet.PktVideo, Data: []byte{0x27, 1, 0, 0, 40, 1}}, 80))
	require.NoError(test, mixer.Mux(&packet.Packet{Type: packet.PktAudio, Data: []byte{0xaf, 1, 1}}, 90))
	require.NoError(test, mixer.Mux(&packet.Packet{Type: packet.PktVideo, Data: []byte{0x17, 0, 0, 0, 0, 1}}, 999999))
	return output.Bytes()
}

func TestResumeAppendsOnlySequenceHeadersAtRestoredTime(test *testing.T) {
	existing := resumeFixture(test)
	var output bytes.Buffer
	mixer := NewMixer(&output)
	require.NoError(test, mixer.SaveAVCHeader(&packet.Packet{Type: packet.PktVideo, Data: []byte{0x17, 0, 0, 0, 0, 1}}))
	require.NoError(test, mixer.SaveAACHeader(&packet.Packet{Type: packet.PktAudio, Data: []byte{0xaf, 0, 0x12, 0x10}}))
	offset, err := mixer.Resume(bytes.NewReader(existing), int64(len(existing)))
	require.NoError(test, err)
	require.Equal(test, uint32(200), offset)
	appended := output.Bytes()
	require.Equal(test, byte(packet.TagVideo), appended[0])
	require.Equal(test, uint32(200<<8), binary.BigEndian.Uint32(appended[4:8]))
	require.Equal(test, byte(packet.TagAudio), appended[21])
	require.Equal(test, uint32(200<<8), binary.BigEndian.Uint32(appended[25:29]))
	require.Equal(test, 1, bytes.Count(append(bytes.Clone(existing), appended...), []byte("FLV")))
}

func TestResumeRejectsDamagedFLVWithoutWriting(test *testing.T) {
	existing := resumeFixture(test)
	corruptHeader := bytes.Clone(existing)
	corruptHeader[0] = 0
	corruptLength := bytes.Clone(existing)
	corruptLength[len(corruptLength)-1]++
	for _, damaged := range [][]byte{existing[:8], existing[:len(existing)-1], corruptHeader, corruptLength} {
		var output bytes.Buffer
		mixer := NewMixer(&output)
		_, err := mixer.Resume(bytes.NewReader(damaged), int64(len(damaged)))
		require.Error(test, err)
		require.Zero(test, output.Len())
	}
}

func TestResumePreservesExtendedAndSignedFLVTime(test *testing.T) {
	var output bytes.Buffer
	mixer := NewMixer(&output)
	require.NoError(test, mixer.SetFlvHeader())
	for _, timestamp := range []uint32{0x12345650, 0x12345678} {
		require.NoError(test, mixer.Mux(&packet.Packet{Type: packet.PktVideo, Data: []byte{0x27, 1, 0xff, 0xff, 0xd8, 1}}, timestamp))
	}
	offset, err := resumeTimestamp(bytes.NewReader(output.Bytes()), int64(output.Len()))
	require.NoError(test, err)
	require.Equal(test, uint32(0x123456a0), offset)
}
