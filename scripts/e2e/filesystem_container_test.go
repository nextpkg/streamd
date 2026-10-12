package main

import (
	"archive/tar"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMountedRecordingExtractionPreservesBytes(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: "./flv/sample/media.flv", Typeflag: tar.TypeReg, Mode: 0600, Size: 3}))
	_, err := writer.Write([]byte("FLV"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	destination := t.TempDir()
	require.NoError(t, extractRecordings(&archive, destination))
	body, err := os.ReadFile(filepath.Join(destination, "flv/sample/media.flv"))
	require.NoError(t, err)
	require.Equal(t, []byte("FLV"), body)
}

func TestMountedRecordingExtractionRejectsUnsafeOrEmptyArchives(t *testing.T) {
	for _, header := range []*tar.Header{
		{Name: "../escape", Typeflag: tar.TypeReg}, {Name: "/absolute", Typeflag: tar.TypeReg},
		{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/tmp"}, {Name: "oversize", Typeflag: tar.TypeReg, Size: 41 * 1024 * 1024},
	} {
		var archive bytes.Buffer
		writer := tar.NewWriter(&archive)
		require.NoError(t, writer.WriteHeader(header))
		_ = writer.Close()
		require.Error(t, extractRecordings(&archive, t.TempDir()))
	}
	require.Error(t, extractRecordings(bytes.NewReader(nil), t.TempDir()))
}

func TestFilesystemRollbackRequiresOnDiskEvidence(t *testing.T) {
	for _, example := range []struct {
		name   string
		offset int64
		body   string
		exists bool
		valid  bool
	}{
		{name: "new empty removed", valid: true},
		{name: "new empty retained", exists: true},
		{name: "partial tail retained", exists: true, body: "tail"},
		{name: "confirmed prefix preserved", offset: 6, exists: true, body: "prefix", valid: true},
		{name: "confirmed prefix missing", offset: 6},
		{name: "confirmed prefix with partial tail", offset: 6, exists: true, body: "prefixtail"},
	} {
		t.Run(example.name, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "recordings/fault/flv/sample/123.flv")
			if example.exists {
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
				require.NoError(t, os.WriteFile(path, []byte(example.body), 0600))
			}
			message := fmt.Sprintf("recording append failed: written=4096 requested=100000 rollback_offset=%d: write /fault/flv/sample/123.flv: no space left on device", example.offset)
			files, err := verifyFilesystemRollback(directory, []string{message, "[flv/sample]recording failed: " + message})
			if example.valid {
				require.NoError(t, err)
				require.Len(t, files, 1)
				require.Equal(t, example.offset, files[0]["accepted_bytes"])
			} else {
				require.Error(t, err)
			}
		})
	}
	_, err := verifyFilesystemRollback(t.TempDir(), []string{"no space left on device"})
	require.ErrorContains(t, err, "actual partial media write")
}

func TestFilesystemRollbackMeasuresActualPartialExtent(test *testing.T) {
	directory := test.TempDir()
	filename := filepath.Join(directory, "recordings/fault/flv/sample/123.flv")
	require.NoError(test, os.MkdirAll(filepath.Dir(filename), 0700))
	require.NoError(test, os.WriteFile(filename, bytes.Repeat([]byte("p"), 92095), 0600))
	for _, written := range []int{6209, 0, 80996, 80997} {
		message := fmt.Sprintf("recording append failed: written=%d requested=80996 rollback_offset=92095: write /fault/flv/sample/123.flv: no space left on device", written)
		files, err := verifyFilesystemRollback(directory, []string{message})
		if written == 6209 {
			require.NoError(test, err)
			require.Len(test, files, 1)
			require.EqualValues(test, 6209, files[0]["partial_write_bytes"])
			require.EqualValues(test, 92095, files[0]["accepted_bytes"])
		} else {
			require.Error(test, err)
		}
	}
}
