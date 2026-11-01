// Package file is a storage implement
package file

import (
	"io"
	"os"
	"sync"

	"github.com/nextpkg/streamd/internal/errutil"
	"github.com/nextpkg/streamd/record/storage/driver"
)

// Config file driver
type Config struct {
	mu       sync.Mutex
	fd       fileHandle
	path     string
	size     int64
	position int64
	created  bool
	failure  error
}

type fileHandle interface {
	io.ReaderAt
	io.Writer
	io.Seeker
	Truncate(int64) error
	Stat() (os.FileInfo, error)
	Sync() error
	Close() error
}

// NewDriver file driver
func NewDriver(path string) driver.Driver {
	return &Config{
		path: path,
	}
}

// Open file
func (d *Config) Open() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	fd, err1 := os.OpenFile(d.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	created := err1 == nil
	if errutil.Is(err1, os.ErrExist) {
		fd, err1 = os.OpenFile(d.path, os.O_RDWR, 0600)
	}
	if err1 != nil {
		return errutil.Wrap(err1, "open file failed")
	}

	offset, err2 := fd.Seek(0, 2)
	if err2 != nil {
		_ = fd.Close()
		return errutil.Wrapf(err2, "seek failed,offset=%d", offset)
	}

	d.fd = fd
	d.size = offset
	d.position, d.created, d.failure = offset, created, nil
	return nil
}

func (writer *Config) Existing() (io.ReaderAt, int64) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.fd, writer.size
}

// Write data to file
func (writer *Config) Write(data []byte) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.failure != nil {
		return writer.failure
	}
	count, err := writer.fd.Write(data)
	if err == nil && count != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		var rollbackError, seekError error
		if count > 0 {
			rollbackError = writer.fd.Truncate(writer.position)
			_, seekError = writer.fd.Seek(writer.position, io.SeekStart)
		}
		writer.failure = errutil.Wrapf(errutil.Join(err, rollbackError, seekError), "recording append failed: written=%d requested=%d rollback_offset=%d", count, len(data), writer.position)
		return writer.failure
	}
	writer.position += int64(count)
	return nil
}

// Close file
func (writer *Config) Close(sync, block bool) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	var syncError error
	if sync {
		syncError = writer.fd.Sync()
	}
	_ = block
	var removeError error
	var identity os.FileInfo
	if writer.failure != nil && writer.created && writer.position == 0 {
		identity, removeError = writer.fd.Stat()
	}
	closeError := writer.fd.Close()
	if identity != nil && identity.Size() == 0 {
		current, err := os.Lstat(writer.path)
		if err == nil && os.SameFile(identity, current) && current.Size() == 0 {
			removeError = os.Remove(writer.path)
		} else if err != nil && !os.IsNotExist(err) {
			removeError = err
		}
	}
	return errutil.Join(writer.failure, syncError, closeError, removeError)
}
