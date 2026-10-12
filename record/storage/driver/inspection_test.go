package driver_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/record/storage/driver"
)

type delayedInspectionBackend struct {
	objectstore.Backend
	entered, release chan struct{}
	once             sync.Once
}

func (backend *delayedInspectionBackend) Uploads(ctx context.Context, prefix string) ([]objectstore.Upload, error) {
	if prefix == "inspection/sample/" {
		backend.once.Do(func() { close(backend.entered) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-backend.release:
		}
	}
	return nil, nil
}
func (*delayedInspectionBackend) ReadCheckpoint(context.Context, string) ([]byte, error) {
	return nil, objectstore.ErrCheckpointNotFound
}
func (*delayedInspectionBackend) DeleteCheckpoint(context.Context, string) error        { return nil }
func (*delayedInspectionBackend) WriteCheckpoint(context.Context, string, []byte) error { return nil }
func (*delayedInspectionBackend) Create(context.Context, string) (string, error) {
	return "fresh-upload", nil
}
func (*delayedInspectionBackend) Abort(context.Context, string, string) error { return nil }

func TestObjectOpenDoesNotWaitForDirectoryInspection(test *testing.T) {
	backend := &delayedInspectionBackend{entered: make(chan struct{}), release: make(chan struct{})}
	client := &objectstore.Client{Backend: backend}
	defer client.Close()
	writer := driver.NewObject(client, "inspection/sample/2000000000.ts", 900, true)
	opened := make(chan error, 1)
	go func() { opened <- writer.Open() }()
	var openError error
	select {
	case openError = <-opened:
	case <-time.After(200 * time.Millisecond):
		test.Error("opening a new recording waited for unrelated directory inspection")
		close(backend.release)
		select {
		case openError = <-opened:
		case <-time.After(time.Second):
			openError = errors.New("Open did not retire after releasing inspection")
		}
	}
	if openError != nil {
		test.Fatal(openError)
	}
	if err := writer.Close(true, true); err != nil {
		test.Fatal(err)
	}
}

type failedOpenBackend struct{ delayedInspectionBackend }

func (*failedOpenBackend) ReadCheckpoint(context.Context, string) ([]byte, error) {
	return nil, errors.New("injected current-object checkpoint failure")
}

func TestFailedObjectOpenStillSchedulesIndependentMaintenance(test *testing.T) {
	client := &objectstore.Client{Backend: &failedOpenBackend{}}
	defer client.Close()
	writer := driver.NewObject(client, "failed/2000000000.ts", 900, true)
	defer writer.Close(true, true)
	if err := writer.Open(); err == nil {
		test.Fatal("current object unexpectedly opened")
	}
	if stats := client.InspectionStats(); stats.Pending != 1 {
		test.Fatalf("current-object failure prevented independent maintenance: %+v", stats)
	}
}
