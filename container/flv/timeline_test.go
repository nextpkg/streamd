package flv

import (
	"bytes"
	"testing"

	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func TestForwardTimelineAgreesWithEncodedFLVTail(test *testing.T) {
	var output bytes.Buffer
	mixer := NewMixer(&output)
	require.NoError(test, mixer.SetFlvHeader())
	for _, sample := range []struct {
		timestamp uint32
		data      []byte
	}{{40, []byte{0x27, 1, 0, 0, 120, 1}}, {80, []byte{0x27, 1, 0, 0, 40, 1}}, {999999, []byte{0x17, 0, 0, 0, 0, 1}}} {
		require.NoError(test, mixer.Mux(&packet.Packet{Type: packet.PktVideo, Data: sample.data}, sample.timestamp))
	}
	parsed, err := resumeTimestamp(bytes.NewReader(output.Bytes()), int64(output.Len()))
	require.NoError(test, err)
	require.Equal(test, uint64(parsed), mixer.NextTimestamp())
	require.Equal(test, uint64(200), mixer.NextTimestamp())
	_, err = mixer.ResumeAt(parsed)
	require.NoError(test, err)
	require.Zero(test, mixer.NextTimestamp())
	before := output.Len()
	_, err = mixer.ResumeAt(0)
	require.Error(test, err)
	require.Equal(test, before, output.Len())
}
