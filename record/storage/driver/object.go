package driver

import (
	"context"
	"time"

	"github.com/nextpkg/streamd/internal/mediatime"
	"github.com/nextpkg/streamd/internal/objectstore"
)

type object struct {
	client    *objectstore.Client
	upload    *objectstore.MultipartUpload
	key       string
	segment   int64
	resume    bool
	protected bool
	release   func()
}

func NewObject(client *objectstore.Client, key string, segment int64, resume bool) Driver {
	return NewConfig(&object{client: client, key: client.CleanKey(key), segment: segment, resume: resume})
}

func (object *object) Open() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	object.client.ScheduleInspection(object.key, object.segment, IsExpired)
	var err error
	object.upload, object.release, err = object.client.NewRecordingUpload(ctx, object.key, object.resume)
	if err == nil {
		object.protected = object.upload.Checkpointed()
	}
	return err
}

func (object *object) ResumeTail() []byte { return object.upload.TakeTail() }

func (object *object) Suspend(tail []byte) error {
	return object.SuspendMedia(tail, nil)
}

func (object *object) ResumeMedia() (*mediatime.State, int64) { return object.upload.ResumeMedia() }

func (object *object) SuspendMedia(tail []byte, state *mediatime.State) error {
	defer object.release()
	object.protected = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return object.upload.SuspendMedia(ctx, tail, state)
}

func (object *object) Upload(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return object.upload.Upload(ctx, data)
}

func (object *object) Complete() error {
	defer object.release()
	object.protected = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return object.upload.Complete(ctx)
}

func (object *object) Abort() error {
	defer object.release()
	if object.protected {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return object.upload.Abort(ctx)
}
