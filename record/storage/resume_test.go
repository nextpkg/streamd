package storage

import (
	"io"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/mediatime"
	"github.com/nextpkg/streamd/metrics"
	"github.com/stretchr/testify/require"
)

type resumeDriver struct {
	state            *mediatime.State
	size             int64
	data             []byte
	closed, complete bool
}

func (driver *resumeDriver) Open() error { return nil }
func (driver *resumeDriver) Write(data []byte) error {
	driver.data = append(driver.data, data...)
	return nil
}
func (driver *resumeDriver) Close(complete, block bool) error {
	driver.complete, driver.closed = complete, true
	return nil
}
func (driver *resumeDriver) ResumeMedia() (*mediatime.State, int64) {
	return driver.state.Clone(), driver.size
}

func TestFailedMediaRestoreCannotAppendOrFinalize(test *testing.T) {
	for _, state := range []*mediatime.State{nil, {Format: "ts", Bytes: 10, Next: 200}, {Format: "flv", Bytes: 9, Next: 200}} {
		driver := &resumeDriver{state: state, size: 10}
		store := NewStorage(base.NewInfo("test", "sample", false), &config.AppRecorder{SaveAs: "flv", Driver: "aws", SaveMode: "round", Segment: 900}, NewEvent())
		store.dr = driver
		store.isOpen.Store(true)
		metrics.AddGlobal(metrics.Recording, 1)
		initialized, resumed := false, false
		_, err := store.Resume(func(io.ReaderAt, int64) (uint32, error) { initialized = true; return 0, nil }, func(offset uint32) (uint32, error) { resumed = true; return offset, nil })
		require.Error(test, err)
		require.False(test, initialized)
		require.False(test, resumed)
		require.Error(test, store.Write([]byte("unsafe append"), 300))
		store.ForceCompletion()
		require.Error(test, store.Close(true))
		require.True(test, driver.closed)
		require.False(test, driver.complete)
		require.Empty(test, driver.data)
	}
}

func TestMediaRestoreUsesPersistedOffsetNotFreshHeader(test *testing.T) {
	driver := &resumeDriver{state: &mediatime.State{Format: "flv", Bytes: 10, Next: 200}, size: 10}
	store := NewStorage(base.NewInfo("test", "sample", false), &config.AppRecorder{SaveAs: "flv"}, NewEvent())
	store.dr = driver
	initialized := false
	offset, err := store.Resume(func(io.ReaderAt, int64) (uint32, error) { initialized = true; return 0, nil }, func(offset uint32) (uint32, error) { return offset, nil })
	require.NoError(test, err)
	require.Equal(test, uint32(200), offset)
	require.False(test, initialized)
}
