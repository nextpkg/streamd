package ts

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResumeAtRestartsTSMuxerWithoutResettingMediaOffset(test *testing.T) {
	var output bytes.Buffer
	mixer := NewMixer(&output)
	mixer.timeline.ObserveForward(videoPID, 999, 999)
	offset, err := mixer.ResumeAt(200)
	require.NoError(test, err)
	require.Equal(test, uint32(200), offset)
	require.Zero(test, mixer.NextTimestamp())
	require.Equal(test, 8*tsPacketLen, output.Len())
	before := output.Len()
	_, err = mixer.ResumeAt(0)
	require.Error(test, err)
	require.Equal(test, before, output.Len())
}
