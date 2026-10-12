package base

import (
	"testing"

	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func TestPublisherRebaseFollowsPresentationAndCadence(t *testing.T) {
	active := NewRwAlive()
	frame := &packet.Packet{Type: packet.PktVideo, Baseline: 1000, Data: []byte{0x27, 1, 0, 0, 80}}
	require.NoError(t, flv.NewDemuxer().Demux(frame))
	active.SetMediaTime(frame)
	frame = &packet.Packet{Type: packet.PktVideo, Baseline: 1040, Data: []byte{0x27, 1, 0, 0, 0}}
	require.NoError(t, flv.NewDemuxer().Demux(frame))
	active.SetMediaTime(frame)
	active.RebaseAfterPresentation()
	require.Equal(t, uint32(1120), active.GetBaseTime())
	header := &packet.Packet{Type: packet.PktVideo, Baseline: 0, Data: []byte{0x17, 0, 0, 0, 0}}
	require.NoError(t, flv.NewDemuxer().Demux(header))
	active.SetMediaTime(header)
	require.Equal(t, uint32(1040), active.LastVideoTime())
}
