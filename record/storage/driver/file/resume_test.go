package file

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExistingSnapshotDoesNotMoveAppendOffset(test *testing.T) {
	filename := filepath.Join(test.TempDir(), "recording.flv")
	require.NoError(test, os.WriteFile(filename, []byte("prefix"), 0600))
	driver := NewDriver(filename)
	require.NoError(test, driver.Open())
	reader, size := driver.(interface{ Existing() (io.ReaderAt, int64) }).Existing()
	require.Equal(test, int64(6), size)
	data := make([]byte, size)
	_, err := reader.ReadAt(data, 0)
	require.NoError(test, err)
	require.Equal(test, []byte("prefix"), data)
	require.NoError(test, driver.Write([]byte("tail")))
	require.NoError(test, driver.Close(true, true))
	data, err = os.ReadFile(filename)
	require.NoError(test, err)
	require.Equal(test, []byte("prefixtail"), data)
}
