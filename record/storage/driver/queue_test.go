package driver

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

type fakeUpload struct {
	data      []byte
	err       error
	completed bool
	aborted   bool
	gate      chan struct{}
	started   chan struct{}
}

func (upload *fakeUpload) Open() error { return nil }
func (upload *fakeUpload) Upload(data []byte) error {
	if upload.started != nil {
		close(upload.started)
		upload.started = nil
	}
	if upload.gate != nil {
		<-upload.gate
	}
	if upload.err != nil {
		return upload.err
	}
	upload.data = append(upload.data, data...)
	return nil
}
func (upload *fakeUpload) Complete() error { upload.completed = true; return nil }
func (upload *fakeUpload) Abort() error    { upload.aborted = true; return nil }

func TestUploadQueueFlushAndFailure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		upload := &fakeUpload{}
		if failed {
			upload.err = errors.New("injected upload failure")
		}
		driver := NewConfig(upload)
		if err := driver.Open(); err != nil {
			t.Fatal(err)
		}
		if err := driver.Write([]byte("media")); err != nil {
			t.Fatal(err)
		}
		err := driver.Close(true, true)
		if failed {
			if !errors.Is(err, upload.err) || upload.completed || !upload.aborted {
				t.Fatal("failed recording was falsely completed")
			}
		} else if err != nil || !upload.completed || !bytes.Equal(upload.data, []byte("media")) {
			t.Fatal("recording tail was not flushed")
		}
		if err := driver.Write([]byte("late")); err == nil {
			t.Fatal("write after close accepted")
		}
	}
}

func TestUploadQueueBackpressureDoesNotBlock(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{})
	upload := &fakeUpload{gate: gate, started: started}
	driver := NewConfig(upload)
	if err := driver.Open(); err != nil {
		t.Fatal(err)
	}
	block := make([]byte, blockSize)
	if err := driver.Write(block); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("uploader never started")
	}
	for index := 0; index < ioChanSize; index++ {
		if err := driver.Write(block); err != nil {
			t.Fatal(err)
		}
	}
	begin := time.Now()
	if err := driver.Write(block); err == nil {
		t.Fatal("unbounded recording queue")
	}
	if time.Since(begin) > time.Second {
		t.Fatal("backpressure blocked the producer")
	}
	close(gate)
	if err := driver.Close(true, true); err == nil || upload.completed {
		t.Fatal("overflow did not mark recording failed")
	}
}
