package item

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

type disconnectedResponse struct{ writes int }

func (response *disconnectedResponse) Header() http.Header { return make(http.Header) }
func (response *disconnectedResponse) WriteHeader(int)     {}
func (response *disconnectedResponse) Write([]byte) (int, error) {
	response.writes++
	return 0, syscall.EPIPE
}

func TestResponseDisconnectClassificationDoesNotHideOtherErrors(t *testing.T) {
	for _, failure := range []error{context.Canceled, net.ErrClosed, syscall.EPIPE, syscall.ECONNRESET} {
		require.True(t, clientDisconnected(fmt.Errorf("HTTP response: %w", failure)))
	}
	for _, failure := range []error{nil, io.ErrShortWrite, context.DeadlineExceeded, errors.New("broken pipe"), errors.New("media corruption")} {
		require.False(t, clientDisconnected(failure))
	}
}

func TestDisconnectedSegmentResponsePreservesCachedMedia(t *testing.T) {
	segment := NewTsItem("/segment.ts", 2000, 1, []byte{0x47, 1, 2, 3})
	response := &disconnectedResponse{}
	segment.Response(response)
	require.Equal(t, 1, response.writes)
	recovered := httptest.NewRecorder()
	segment.Response(recovered)
	require.Equal(t, []byte{0x47, 1, 2, 3}, recovered.Body.Bytes())
}
