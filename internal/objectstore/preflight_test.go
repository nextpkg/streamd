package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

type interruptedProbe struct {
	*checkpointStub
	cancel              context.CancelFunc
	cleanupContextError error
	cleanupBudget       time.Duration
	deletes             int
}

func (backend *interruptedProbe) WriteCheckpoint(ctx context.Context, key string, body []byte) error {
	if err := backend.checkpointStub.WriteCheckpoint(ctx, key, body); err != nil {
		return err
	}
	backend.cancel()
	return ctx.Err()
}

func (backend *interruptedProbe) DeleteCheckpoint(ctx context.Context, key string) error {
	backend.deletes++
	backend.cleanupContextError = ctx.Err()
	if deadline, exists := ctx.Deadline(); exists {
		backend.cleanupBudget = time.Until(deadline)
	}
	return backend.checkpointStub.DeleteCheckpoint(ctx, key)
}

func TestPreflightCancellationCleansUncertainWriteWithBoundedContext(test *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backend := &interruptedProbe{checkpointStub: &checkpointStub{}, cancel: cancel}
	client := &Client{Backend: backend}
	err := client.Preflight(ctx, "recordings/live")
	if !errors.Is(err, context.Canceled) || backend.saved != nil || backend.deletes != 1 || backend.cleanupContextError != nil || backend.cleanupBudget <= 0 || backend.cleanupBudget > time.Second {
		test.Fatalf("uncertain write cleanup: err=%v backend=%+v", err, backend)
	}
}

func TestPreflightAlreadyCancelledDoesNotAccessBackend(test *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &Client{}
	if err := client.Preflight(ctx, "recordings/live"); !errors.Is(err, context.Canceled) {
		test.Fatalf("cancelled probe: %v", err)
	}
}
