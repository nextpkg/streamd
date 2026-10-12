package core

import (
	"bytes"
	"io"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

type ackTransport struct {
	input   io.Reader
	output  bytes.Buffer
	failure error
}

func (transport *ackTransport) Read(data []byte) (int, error) { return transport.input.Read(data) }
func (transport *ackTransport) Write(data []byte) (int, error) {
	if transport.failure != nil {
		return 0, transport.failure
	}
	return transport.output.Write(data)
}

func ackTestConnection(failure error) (*Conn, *ackTransport) {
	transport := &ackTransport{input: bytes.NewReader([]byte{6, 0, 0, 0, 0, 0, 1, 9, 1, 0, 0, 0, 0x17}), failure: failure}
	return &Conn{slab: NewSlab(), rw: NewReadWriter(transport, 1), chunkSize: 128, remoteChunkSize: 128, windowAckSize: 2500000,
		remoteWindowAckSize: 1, option: defaultOption, chunks: make(map[uint32]*ChunkStream)}, transport
}

func TestReadPropagatesAcknowledgementFailure(t *testing.T) {
	connection, _ := ackTestConnection(syscall.EPIPE)
	chunk := ChunkStream{typeID: 77}
	err := connection.Read(&chunk)
	require.ErrorIs(t, err, syscall.EPIPE)
	require.Equal(t, uint32(77), chunk.typeID, "failed reads must not publish an apparently valid chunk")
	require.Equal(t, uint32(1), connection.ackReceived, "a failed acknowledgement must not be counted as sent")
}

func TestReadAcknowledgementSuccessStillDeliversMedia(t *testing.T) {
	connection, transport := ackTestConnection(nil)
	var chunk ChunkStream
	require.NoError(t, connection.Read(&chunk))
	require.NoError(t, connection.Flush())
	require.Equal(t, uint32(9), chunk.typeID)
	require.Equal(t, []byte{0x17}, chunk.data)
	require.Equal(t, uint32(0), connection.ackReceived)
	require.NotEmpty(t, transport.output.Bytes())
}
