package record

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/appstate"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

type blockedSegment struct {
	started    chan struct{}
	release    chan struct{}
	once       sync.Once
	unlockOnce sync.Once
	failure    error
}

func (segment *blockedSegment) Write(*packet.Packet) error { return segment.failure }
func (segment *blockedSegment) After() {
	segment.once.Do(func() { close(segment.started) })
	<-segment.release
}
func (segment *blockedSegment) Finish() error {
	segment.After()
	return segment.failure
}

func (segment *blockedSegment) unlock() { segment.unlockOnce.Do(func() { close(segment.release) }) }

func recordingState(info *base.Info) appstate.Status {
	for _, state := range appstate.Snapshot() {
		if state.Component == "recording" && state.Stream == info.Key {
			return state
		}
	}
	return appstate.Status{}
}

func TestRecordingFinalizationDoesNotBlockDisable(t *testing.T) {
	info := base.NewInfo(t.Name(), "sample", false)
	config.Get().SetApp(info.App, &config.App{Enable: true, AppName: info.App})
	writer := newSwitchingWriter(info, &base.ConnectInfo{})
	segment := &blockedSegment{started: make(chan struct{}), release: make(chan struct{}), failure: errors.New("storage finalization unavailable")}
	writer.terminal = segment
	finished := make(chan struct{})
	keyframe := recorderPacket(t, packet.PktVideo, 2000, []byte{0x17, 1, 0, 0, 0, 0, 0, 0, 1, 0x65})
	defer func() { segment.unlock(); <-finished; writer.After() }()
	go func() { _ = writer.Write(keyframe); close(finished) }()
	select {
	case <-segment.started:
	case <-time.After(time.Second):
		t.Fatal("finalization never started")
	}
	select {
	case <-finished:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("storage finalization blocked hot disable")
	}
	require.Equal(t, "disabled", recordingState(info).State)
	segment.unlock()
	<-writer.finalizing[0].done
	require.Equal(t, "disabled", recordingState(info).State, "late cleanup failure must not undo the new configuration")
	require.Equal(t, config.Get().Revision, recordingState(info).Revision)
	require.Equal(t, uint64(1), recordingState(info).Failures)
	require.Positive(t, recordingState(info).LastFailureAt)
}

func TestRecordingFailureReportedBeforeBlockedCleanup(t *testing.T) {
	info := base.NewInfo(t.Name(), "sample", false)
	writer := newSwitchingWriter(info, &base.ConnectInfo{})
	segment := &blockedSegment{started: make(chan struct{}), release: make(chan struct{}), failure: errors.New("upload queue capacity exceeded")}
	writer.terminal = segment
	finished := make(chan struct{})
	frame := recorderPacket(t, packet.PktVideo, 2040, []byte{0x27, 1, 0, 0, 0, 0, 0, 0, 1, 0x41})
	defer func() { segment.unlock(); <-finished; writer.After() }()
	go func() { _ = writer.Write(frame); close(finished) }()
	select {
	case <-segment.started:
	case <-time.After(time.Second):
		t.Fatal("cleanup never started")
	}
	require.Equal(t, "failed", recordingState(info).State, "failure must be visible while cleanup is blocked")
	require.Equal(t, uint64(1), recordingState(info).Failures)
	select {
	case <-finished:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("failure cleanup blocked packet processing")
	}
}

func TestRecordingFinalizationsBoundedAndJoined(t *testing.T) {
	info := base.NewInfo(t.Name(), "sample", false)
	writer := newSwitchingWriter(info, &base.ConnectInfo{})
	var segments []*blockedSegment
	defer func() {
		for _, segment := range segments {
			segment.unlock()
		}
		writer.After()
	}()
	for index := 0; index < maxFinalizingRecordings+1; index++ {
		segment := &blockedSegment{started: make(chan struct{}), release: make(chan struct{})}
		segments = append(segments, segment)
		writer.terminal = segment
		require.Equal(t, index < maxFinalizingRecordings, writer.retire())
	}
	require.Len(t, writer.finalizing, maxFinalizingRecordings)
	require.Same(t, segments[len(segments)-1], writer.terminal, "a saturated retirement pool must retain ownership")
	closed := make(chan struct{})
	go func() { writer.After(); close(closed) }()
	segments[len(segments)-1].unlock()
	select {
	case <-closed:
		t.Fatal("shutdown failed to join the pending finalizations")
	case <-time.After(50 * time.Millisecond):
	}
	for _, segment := range segments {
		segment.unlock()
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after cleanup recovered")
	}
}

func TestRecordingWaitsBeforeReopeningFinalizingTarget(t *testing.T) {
	info := base.NewInfo(t.Name(), "sample", false)
	settings := config.AppRecorder{Enable: true, Driver: "fs", SaveAs: "flv", SaveMode: "round", Segment: 3600, SaveTo: t.TempDir()}
	config.Get().SetApp(info.App, &config.App{Enable: true, AppName: info.App, Recorder: settings})
	writer := newSwitchingWriter(info, &base.ConnectInfo{})
	seedRecorder(t, writer)
	segment := &blockedSegment{started: make(chan struct{}), release: make(chan struct{})}
	writer.terminal, writer.settings = segment, &settings
	defer func() { segment.unlock(); writer.After() }()
	keyframe := recorderPacket(t, packet.PktVideo, 2000, []byte{0x17, 1, 0, 0, 0, 0, 0, 0, 1, 0x65})
	require.NoError(t, writer.Write(keyframe))
	require.Nil(t, writer.terminal)
	require.Equal(t, "waiting_finalization", recordingState(info).State)
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(settings.SaveTo, alias))
	aliased := settings
	aliased.SaveTo = alias
	require.True(t, writer.finalizingTarget(&aliased))
	segment.unlock()
	<-writer.finalizing[0].done
	require.NoError(t, writer.Write(keyframe))
	require.NotNil(t, writer.terminal)
	require.Equal(t, "active", recordingState(info).State)
}
