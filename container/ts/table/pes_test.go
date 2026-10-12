package table

import (
	"testing"

	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func TestPESVideoAlwaysCarriesExplicitDTS(test *testing.T) {
	for _, presentation := range []int64{90000, 93600} {
		pes := NewPes()
		require.Equal(test, 19, pes.GeneratePesHeader(packet.PktVideo, 10, presentation, 90000))
		require.Equal(test, byte(0xc0), pes.PesHeader[7])
		require.Equal(test, byte(10), pes.PesHeader[8])
		require.Equal(test, byte(3), pes.PesHeader[9]>>4)
		require.Equal(test, byte(1), pes.PesHeader[14]>>4)
		require.Equal(test, 14, pes.GeneratePesHeader(packet.PktAudio, 10, presentation, presentation))
		require.Equal(test, byte(0x80), pes.PesHeader[7])
		require.Equal(test, byte(2), pes.PesHeader[9]>>4)
	}
}

func TestPESTimestampWrapsAt33Bits(test *testing.T) {
	pes := NewPes()
	for _, timestamp := range []int64{0, 1, 0x1ffffffff, 0x200000000, 0x400000001, -1} {
		encoded := pes.encodeTs(0xc0, timestamp)
		decoded := int64(encoded[0]&0x0e)<<29 | int64(encoded[1])<<22 | int64(encoded[2]&0xfe)<<14 | int64(encoded[3])<<7 | int64(encoded[4])>>1
		require.Equal(test, timestamp&0x1ffffffff, decoded)
		require.Equal(test, byte(1), encoded[0]&1)
		require.Equal(test, byte(1), encoded[2]&1)
		require.Equal(test, byte(1), encoded[4]&1)
	}
}
