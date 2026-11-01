package item

import (
	"context"
	"errors"
	"net"
	"syscall"

	"github.com/nextpkg/streamd/internal/logging"
)

func clientDisconnected(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET)
}

func logResponseError(err error) {
	if clientDisconnected(err) {
		logging.Debug("HLS client disconnected: ", err)
		return
	}
	logging.Error(err)
}
