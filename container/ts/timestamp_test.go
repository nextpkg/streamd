package ts

import (
	"io"
	"testing"

	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func TestMixerPreservesSignedCompositionAndLongStreamTime(t *testing.T) {
	mixer := NewMixer(io.Discard)
	for _, timestamp := range []uint32{0, 2000, 60 * 60 * 1000, 2 * 60 * 60 * 1000} {
		for _, composition := range []int32{-80, -40, 0, 40, 80, 160} {
			require.NoError(t, mixer.Update(&packet.Packet{Type: packet.PktVideo}, timestamp, uint32(composition)))
			require.Equal(t, int64(timestamp)*90, mixer.dts)
			require.Equal(t, (int64(timestamp)+int64(composition))*90, mixer.pts)
		}
	}
}
