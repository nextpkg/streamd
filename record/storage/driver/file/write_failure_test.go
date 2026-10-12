package file

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingFile struct {
	*os.File
	limit         int
	failure       error
	rollbackError error
	writes        int
}

func (file *failingFile) Write(data []byte) (int, error) {
	file.writes++
	count, err := file.File.Write(data[:min(len(data), file.limit)])
	if err != nil {
		return count, err
	}
	return count, file.failure
}
func (file *failingFile) Truncate(size int64) error {
	if file.rollbackError != nil {
		return file.rollbackError
	}
	return file.File.Truncate(size)
}

func TestFailedAppendRollsBackAndRejectsFurtherWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "media.flv")
	writer := NewDriver(path).(*Config)
	require.NoError(t, writer.Open())
	require.NoError(t, writer.Write([]byte("confirmed")))
	file := &failingFile{File: writer.fd.(*os.File), limit: 2, failure: syscall.ENOSPC}
	writer.fd = file
	defer file.File.Close()
	require.ErrorIs(t, writer.Write([]byte("partial")), syscall.ENOSPC)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("confirmed"), body)
	require.ErrorIs(t, writer.Write([]byte("retry")), syscall.ENOSPC)
	require.Equal(t, 1, file.writes)
	require.ErrorIs(t, writer.Close(true, true), syscall.ENOSPC)
}

func TestFailedEmptyRecordingRemovesOnlyNewOwnedFiles(t *testing.T) {
	for _, existing := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "media.flv")
		if existing {
			require.NoError(t, os.WriteFile(path, nil, 0600))
		}
		writer := NewDriver(path).(*Config)
		require.NoError(t, writer.Open())
		file := &failingFile{File: writer.fd.(*os.File), failure: syscall.ENOSPC}
		writer.fd = file
		defer file.File.Close()
		require.ErrorIs(t, writer.Write([]byte("media")), syscall.ENOSPC)
		require.ErrorIs(t, writer.Close(true, true), syscall.ENOSPC)
		_, err := os.Stat(path)
		if existing {
			require.NoError(t, err)
		} else {
			require.True(t, os.IsNotExist(err))
		}
	}
}

func TestFailedEmptyRecordingPreservesReplacementPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "media.flv")
	writer := NewDriver(path).(*Config)
	require.NoError(t, writer.Open())
	file := &failingFile{File: writer.fd.(*os.File), failure: syscall.ENOSPC}
	writer.fd = file
	defer file.File.Close()
	require.ErrorIs(t, writer.Write([]byte("media")), syscall.ENOSPC)
	require.NoError(t, os.Rename(path, path+".original"))
	require.NoError(t, os.WriteFile(path, []byte("replacement"), 0600))
	require.ErrorIs(t, writer.Close(true, true), syscall.ENOSPC)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("replacement"), body)
}

func TestFailedRollbackRemainsObservable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "media.flv")
	writer := NewDriver(path).(*Config)
	require.NoError(t, writer.Open())
	file := &failingFile{File: writer.fd.(*os.File), limit: 2, failure: syscall.ENOSPC, rollbackError: io.ErrUnexpectedEOF}
	writer.fd = file
	defer file.File.Close()
	err := writer.Write([]byte("partial"))
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.ErrorIs(t, writer.Close(true, true), io.ErrUnexpectedEOF)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("pa"), body, "failed rollback must not delete unverified nonempty media")
}

func TestShortFileWriteWithoutErrorIsNotAccepted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "media.ts")
	writer := NewDriver(path).(*Config)
	require.NoError(t, writer.Open())
	require.NoError(t, writer.Write([]byte("prefix")))
	file := &failingFile{File: writer.fd.(*os.File), limit: 2}
	writer.fd = file
	defer file.File.Close()
	require.ErrorIs(t, writer.Write([]byte("partial")), io.ErrShortWrite)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("prefix"), body)
	require.ErrorIs(t, writer.Close(true, true), io.ErrShortWrite)
}

func TestConcurrentFileAppendsPreserveAcceptedExtent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "media.flv")
	writer := NewDriver(path).(*Config)
	require.NoError(t, writer.Open())
	var workers sync.WaitGroup
	failures := make(chan error, 32)
	for index := 0; index < 32; index++ {
		workers.Go(func() { failures <- writer.Write([]byte("accepted")) })
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	file := &failingFile{File: writer.fd.(*os.File), limit: 2, failure: syscall.ENOSPC}
	writer.fd = file
	require.ErrorIs(t, writer.Write([]byte("partial")), syscall.ENOSPC)
	require.ErrorIs(t, writer.Close(true, true), syscall.ENOSPC)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("accepted", 32), string(body))
}
