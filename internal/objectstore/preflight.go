package objectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path"
	"time"
)

type preflightFailure struct {
	operation string
	cause     error
}

func (failure *preflightFailure) Error() string {
	return "object storage preflight " + failure.operation + " failed"
}

func (failure *preflightFailure) Unwrap() error { return failure.cause }

func (client *Client) Preflight(ctx context.Context, prefix string) (result error) {
	if err := ctx.Err(); err != nil {
		return &preflightFailure{operation: "cancelled preparation", cause: err}
	}
	backend, err := client.checkpointBackend()
	if err != nil {
		return err
	}
	probeID := rand.Text()
	key := client.CleanKey(path.Join(prefix, ".streamd-preflight", ".streamd-checkpoints", probeID+".json"))
	if _, err := client.Backend.Uploads(ctx, key); err != nil {
		return &preflightFailure{operation: "listing", cause: err}
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if err := backend.DeleteCheckpoint(cleanupCtx, key); err != nil {
			result = errors.Join(result, &preflightFailure{operation: "cleanup (probe " + probeID + ")", cause: err})
		}
	}()
	payload := []byte(fmt.Sprintf("{\"probe\":%q}", probeID))
	if err := backend.WriteCheckpoint(ctx, key, payload); err != nil {
		return &preflightFailure{operation: "write", cause: err}
	}
	actual, err := backend.ReadCheckpoint(ctx, key)
	if err != nil {
		return &preflightFailure{operation: "read", cause: err}
	}
	if !bytes.Equal(actual, payload) {
		return &preflightFailure{operation: "readback verification", cause: errors.New("probe content mismatch")}
	}
	return nil
}
