package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"

	"github.com/nextpkg/streamd/internal/mediatime"
)

const maxCheckpointSize = 12 * 1024 * 1024

var ErrCheckpointNotFound = errors.New("recording checkpoint not found")

type checkpointBackend interface {
	ReadCheckpoint(context.Context, string) ([]byte, error)
	WriteCheckpoint(context.Context, string, []byte) error
	DeleteCheckpoint(context.Context, string) error
}

type checkpoint struct {
	Version     int              `json:"version"`
	Key         string           `json:"key"`
	ID          string           `json:"upload_id"`
	Parts       []Part           `json:"parts"`
	Tail        []byte           `json:"tail"`
	Digest      string           `json:"tail_sha256"`
	Media       *mediatime.State `json:"media,omitempty"`
	MediaDigest string           `json:"media_sha256,omitempty"`
}

func checkpointKey(key string) string {
	digest := sha256.Sum256([]byte(key))
	return path.Join(path.Dir(key), ".streamd-checkpoints", fmt.Sprintf("%x.json", digest))
}

func readCheckpointBody(body io.ReadCloser) ([]byte, error) {
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, maxCheckpointSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxCheckpointSize {
		return nil, errors.New("recording checkpoint exceeds size limit")
	}
	return data, nil
}

func (client *Client) checkpointBackend() (checkpointBackend, error) {
	backend, ok := client.Backend.(checkpointBackend)
	if !ok {
		return nil, errors.New("object backend does not support durable recording checkpoints")
	}
	return backend, nil
}

func (client *Client) loadCheckpoint(ctx context.Context, key string) (*checkpoint, error) {
	backend, err := client.checkpointBackend()
	if err != nil {
		return nil, err
	}
	data, err := backend.ReadCheckpoint(ctx, checkpointKey(key))
	if errors.Is(err, ErrCheckpointNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read recording checkpoint: %w", err)
	}
	if len(data) > maxCheckpointSize {
		return nil, errors.New("recording checkpoint exceeds size limit")
	}
	var saved checkpoint
	if err := json.Unmarshal(data, &saved); err != nil {
		return nil, fmt.Errorf("decode recording checkpoint: %w", err)
	}
	if saved.Version != 1 || saved.Key != key || saved.ID == "" || len(saved.Parts) > 10000 || len(saved.Tail) >= MinBlockSize {
		return nil, errors.New("invalid recording checkpoint identity or limits")
	}
	if saved.Digest != fmt.Sprintf("%x", sha256.Sum256(saved.Tail)) {
		return nil, errors.New("recording checkpoint tail checksum mismatch")
	}
	if err := validateParts(saved.Parts); err != nil {
		return nil, err
	}
	if saved.Media != nil {
		if err := saved.Media.Validate(partBytes(saved.Parts) + int64(len(saved.Tail))); err != nil {
			return nil, err
		}
		if saved.MediaDigest != mediaDigest(saved.Media) {
			return nil, errors.New("media checkpoint checksum mismatch")
		}
	} else if saved.MediaDigest != "" {
		return nil, errors.New("media checksum has no media checkpoint")
	}
	return &saved, nil
}

func partBytes(parts []Part) int64 {
	var size int64
	for _, part := range parts {
		size += part.Size
	}
	return size
}

func mediaDigest(state *mediatime.State) string {
	data, _ := json.Marshal(state)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func validateParts(parts []Part) error {
	if len(parts) > 10000 {
		return errors.New("multipart part limit exceeded")
	}
	for index, part := range parts {
		if part.Number != int32(index+1) || part.Size < MinBlockSize || part.ETag == "" {
			return errors.New("invalid or undersized resumable multipart manifest")
		}
	}
	return nil
}

func (upload *MultipartUpload) restore(saved *checkpoint) error {
	if saved == nil {
		return errors.New("existing multipart upload has no recording checkpoint; refusing unsafe append")
	}
	if saved.ID != upload.id || !slices.Equal(saved.Parts, upload.parts) {
		return errors.New("recording checkpoint does not match remote upload; refusing unsafe append")
	}
	upload.tail, upload.checkpointed = saved.Tail, true
	upload.media = saved.Media.Clone()
	return nil
}

func (upload *MultipartUpload) TakeTail() []byte {
	tail := upload.tail
	upload.tail = nil
	return tail
}

func (upload *MultipartUpload) Checkpointed() bool { return upload.checkpointed }

func (upload *MultipartUpload) Suspend(ctx context.Context, tail []byte) error {
	return upload.SuspendMedia(ctx, tail, nil)
}

func (upload *MultipartUpload) ResumeMedia() (*mediatime.State, int64) {
	return upload.media.Clone(), partBytes(upload.parts) + int64(len(upload.tail))
}

func (upload *MultipartUpload) SuspendMedia(ctx context.Context, tail []byte, state *mediatime.State) error {
	if len(tail) >= MinBlockSize {
		return errors.New("recording checkpoint tail must be smaller than a multipart block")
	}
	if err := validateParts(upload.parts); err != nil {
		return err
	}
	backend, err := upload.client.checkpointBackend()
	if err != nil {
		return err
	}
	saved := checkpoint{Version: 1, Key: upload.key, ID: upload.id, Parts: upload.parts, Tail: tail, Digest: fmt.Sprintf("%x", sha256.Sum256(tail))}
	if state != nil {
		if err := state.Validate(partBytes(upload.parts) + int64(len(tail))); err != nil {
			return err
		}
		saved.Media, saved.MediaDigest = state.Clone(), mediaDigest(state)
	}
	data, err := json.Marshal(saved)
	if err != nil {
		return err
	}
	if len(data) > maxCheckpointSize {
		return errors.New("recording checkpoint exceeds size limit")
	}
	if err := backend.WriteCheckpoint(ctx, checkpointKey(upload.key), data); err != nil {
		return fmt.Errorf("save recording checkpoint: %w", err)
	}
	upload.checkpointed = true
	return nil
}

func (upload *MultipartUpload) clearCheckpoint(ctx context.Context) error {
	if !upload.checkpointed {
		return nil
	}
	backend, err := upload.client.checkpointBackend()
	if err != nil {
		return err
	}
	if err := backend.DeleteCheckpoint(ctx, checkpointKey(upload.key)); err != nil {
		return fmt.Errorf("remove completed recording checkpoint: %w", err)
	}
	upload.checkpointed = false
	return nil
}
