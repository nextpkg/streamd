package driver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnsupportedUnfinishedTailIsReported(test *testing.T) {
	upload := &fakeUpload{}
	writer := NewConfig(upload)
	require.NoError(test, writer.Open())
	require.NoError(test, writer.Write([]byte("must not silently disappear")))
	require.ErrorContains(test, writer.Close(false, true), "cannot preserve an unfinished tail")
	require.False(test, upload.completed)
	require.True(test, upload.aborted)
}
