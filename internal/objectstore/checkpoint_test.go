package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type checkpointStub struct {
	Backend
	saved     []byte
	readErr   error
	writeErr  error
	deleteErr error
	uploads   []Upload
	parts     []Part
	creates   int
}

func (stub *checkpointStub) ReadCheckpoint(context.Context, string) ([]byte, error) {
	if stub.readErr != nil {
		return nil, stub.readErr
	}
	if stub.saved == nil {
		return nil, ErrCheckpointNotFound
	}
	return bytes.Clone(stub.saved), nil
}

func (stub *checkpointStub) WriteCheckpoint(_ context.Context, _ string, data []byte) error {
	if stub.writeErr != nil {
		return stub.writeErr
	}
	stub.saved = bytes.Clone(data)
	return nil
}

func (stub *checkpointStub) DeleteCheckpoint(context.Context, string) error {
	if stub.deleteErr != nil {
		return stub.deleteErr
	}
	stub.saved = nil
	return nil
}

func (stub *checkpointStub) Uploads(context.Context, string) ([]Upload, error) {
	return stub.uploads, nil
}

func (stub *checkpointStub) Parts(context.Context, string, string) ([]Part, error) {
	return stub.parts, nil
}

func (stub *checkpointStub) Create(context.Context, string) (string, error) {
	stub.creates++
	return "new-upload", nil
}

func validCheckpoint() checkpoint {
	tail := []byte("pending media")
	return checkpoint{
		Version: 1, Key: "app/stream/2000000000.flv", ID: "upload",
		Parts: []Part{{Number: 1, ETag: "etag", Size: MinBlockSize}},
		Tail:  tail, Digest: fmt.Sprintf("%x", sha256.Sum256(tail)),
	}
}

func TestCheckpointRejectsInvalidState(test *testing.T) {
	for _, scenario := range []struct {
		name   string
		modify func(*checkpoint)
	}{
		{"version", func(saved *checkpoint) { saved.Version = 2 }},
		{"wrong-key", func(saved *checkpoint) { saved.Key = "another/object" }},
		{"missing-upload", func(saved *checkpoint) { saved.ID = "" }},
		{"tail-checksum", func(saved *checkpoint) { saved.Tail[0] ^= 0xff }},
		{"oversized-tail", func(saved *checkpoint) { saved.Tail = make([]byte, MinBlockSize) }},
		{"missing-etag", func(saved *checkpoint) { saved.Parts[0].ETag = "" }},
		{"part-gap", func(saved *checkpoint) { saved.Parts[0].Number = 2 }},
		{"undersized-part", func(saved *checkpoint) { saved.Parts[0].Size = MinBlockSize - 1 }},
		{"too-many-parts", func(saved *checkpoint) { saved.Parts = make([]Part, 10001) }},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			saved := validCheckpoint()
			key := saved.Key
			scenario.modify(&saved)
			data, err := json.Marshal(saved)
			require.NoError(test, err)
			backend := &checkpointStub{saved: data}
			client := &Client{Backend: backend}
			_, err = client.loadCheckpoint(context.Background(), key)
			require.Error(test, err)
			require.Zero(test, backend.creates)
		})
	}
	for _, data := range [][]byte{[]byte("invalid"), bytes.Repeat([]byte(" "), maxCheckpointSize+1)} {
		client := &Client{Backend: &checkpointStub{saved: data}}
		_, err := client.loadCheckpoint(context.Background(), validCheckpoint().Key)
		require.Error(test, err)
	}
	denied := errors.New("access denied")
	client := &Client{Backend: &checkpointStub{readErr: denied}}
	_, err := client.loadCheckpoint(context.Background(), validCheckpoint().Key)
	require.ErrorIs(test, err, denied)
}

func TestCheckpointLookupFailsClosed(test *testing.T) {
	for _, scenario := range []struct {
		name   string
		modify func(*checkpointStub)
		resume bool
	}{
		{"missing-checkpoint", func(backend *checkpointStub) { backend.saved = nil }, true},
		{"orphan-checkpoint", func(backend *checkpointStub) { backend.uploads = nil }, true},
		{"wrong-upload", func(backend *checkpointStub) { backend.uploads[0].ID = "other" }, true},
		{"stale-parts", func(backend *checkpointStub) { backend.parts[0].ETag = "changed" }, true},
		{"missing-part", func(backend *checkpointStub) { backend.parts = nil }, true},
		{"ambiguous-upload", func(backend *checkpointStub) {
			backend.uploads = append(backend.uploads, Upload{Key: validCheckpoint().Key, ID: "other"})
		}, true},
		{"resumption-disabled", func(*checkpointStub) {}, false},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			saved := validCheckpoint()
			data, err := json.Marshal(saved)
			require.NoError(test, err)
			backend := &checkpointStub{saved: data, uploads: []Upload{{Key: saved.Key, ID: saved.ID}}, parts: saved.Parts}
			scenario.modify(backend)
			client := &Client{Backend: backend}
			_, err = client.NewMultipartUpload(context.Background(), saved.Key, scenario.resume)
			require.Error(test, err)
			require.Zero(test, backend.creates)
		})
	}
}

func TestCheckpointRestoresExactTailOnce(test *testing.T) {
	saved := validCheckpoint()
	data, err := json.Marshal(saved)
	require.NoError(test, err)
	backend := &checkpointStub{saved: data, uploads: []Upload{{Key: saved.Key, ID: saved.ID}}, parts: saved.Parts}
	client := &Client{Backend: backend}
	upload, err := client.NewMultipartUpload(context.Background(), saved.Key, true)
	require.NoError(test, err)
	require.True(test, upload.Checkpointed())
	require.Equal(test, saved.Tail, upload.TakeTail())
	require.Empty(test, upload.TakeTail())
	require.Zero(test, backend.creates)
	backend.saved, backend.uploads = nil, []Upload{{Key: saved.Key + ".other", ID: "unrelated"}}
	upload, err = client.NewMultipartUpload(context.Background(), saved.Key, false)
	require.NoError(test, err)
	require.False(test, upload.Checkpointed())
	require.Empty(test, upload.TakeTail())
	require.Equal(test, 1, backend.creates)
}

func TestCheckpointSuspendAndCleanupFailures(test *testing.T) {
	saved := validCheckpoint()
	backend := &checkpointStub{}
	client := &Client{Backend: backend}
	upload := &MultipartUpload{client: client, key: saved.Key, id: saved.ID, parts: saved.Parts}
	ctx := context.Background()
	require.Error(test, upload.Suspend(ctx, make([]byte, MinBlockSize)))
	require.Nil(test, backend.saved)
	require.NoError(test, upload.Suspend(ctx, saved.Tail))
	require.True(test, upload.Checkpointed())
	loaded, err := client.loadCheckpoint(ctx, saved.Key)
	require.NoError(test, err)
	require.Equal(test, saved, *loaded)
	previous := bytes.Clone(backend.saved)
	backend.writeErr = errors.New("write failed")
	require.ErrorIs(test, upload.Suspend(ctx, []byte("new tail")), backend.writeErr)
	require.Equal(test, previous, backend.saved)
	backend.deleteErr = errors.New("delete failed")
	require.ErrorIs(test, upload.clearCheckpoint(ctx), backend.deleteErr)
	require.True(test, upload.Checkpointed())
	require.Equal(test, previous, backend.saved)
	backend.deleteErr = nil
	require.NoError(test, upload.clearCheckpoint(ctx))
	require.False(test, upload.Checkpointed())
	require.Nil(test, backend.saved)
}

type checkpointReader struct {
	io.Reader
	closed bool
}

func (reader *checkpointReader) Close() error { reader.closed = true; return nil }

func TestCheckpointReadIsBoundedAndCloses(test *testing.T) {
	for _, size := range []int{0, 1024, maxCheckpointSize, maxCheckpointSize + 1} {
		test.Run(fmt.Sprint(size), func(test *testing.T) {
			body := &checkpointReader{Reader: bytes.NewReader(make([]byte, size))}
			data, err := readCheckpointBody(body)
			if size > maxCheckpointSize {
				require.ErrorContains(test, err, "size limit")
				require.Nil(test, data)
			} else {
				require.NoError(test, err)
				require.Len(test, data, size)
			}
			require.True(test, body.closed)
		})
	}
}
