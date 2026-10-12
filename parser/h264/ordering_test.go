package h264

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func avccUnits(units ...[]byte) []byte {
	var data []byte
	for _, unit := range units {
		data = binary.BigEndian.AppendUint32(data, uint32(len(unit)))
		data = append(data, unit...)
	}
	return data
}

func TestCBRParametersPrecedeBufferingSEIAndFillerIsPreserved(test *testing.T) {
	parameters := []byte{0, 0, 0, 1, 0x67, 0x42, 0, 0, 0, 1, 0x68, 0xce}
	for _, inBand := range []bool{false, true} {
		parser := NewParser()
		parser.specificInfo = parameters
		units := [][]byte{{0x06, 0, 0x80}, {0x65, 0x88}, {0x0c, 0xff, 0x80}}
		if inBand {
			parser.specificInfo = nil
			units = append([][]byte{{0x67, 0x42}, {0x68, 0xce}}, units...)
		}
		var output bytes.Buffer
		require.NoError(test, parser.Parse(avccUnits(units...), false, &output))
		expected := append(bytes.Clone(naluAud), parameters...)
		for _, unit := range [][]byte{{0x06, 0, 0x80}, {0x65, 0x88}, {0x0c, 0xff, 0x80}} {
			expected = append(expected, startCode...)
			expected = append(expected, unit...)
		}
		require.Equal(test, expected, output.Bytes())
	}
}

func TestMalformedAVCCIsRejectedBeforePartialOutput(test *testing.T) {
	for _, data := range [][]byte{avccUnits(nil), {0, 0, 0, 8, 0x65}, append(avccUnits([]byte{0x61, 1}), 0, 0), avccUnits([]byte{0x1f, 1})} {
		var output bytes.Buffer
		require.Error(test, NewParser().Parse(data, false, &output))
		require.Empty(test, output.Bytes())
	}
}
