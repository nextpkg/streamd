package record

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	containerflv "github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/internal/appstate"
	"github.com/nextpkg/streamd/packet"
	recordflv "github.com/nextpkg/streamd/record/flv"
	recordts "github.com/nextpkg/streamd/record/ts"
	"github.com/stretchr/testify/require"
)

func recorderPacket(t *testing.T, kind int, timestamp uint32, data []byte) *packet.Packet {
	t.Helper()
	frame := &packet.Packet{Type: kind, Baseline: timestamp, Data: data}
	require.NoError(t, containerflv.NewDemuxer().Demux(frame))
	return frame
}

func seedRecorder(t *testing.T, writer *switchingWriter) {
	t.Helper()
	video := recorderPacket(t, packet.PktVideo, 0, []byte{
		0x17, 0, 0, 0, 0, 1, 0x64, 0, 0x1f, 0xff, 0xe1, 0, 0x19,
		0x67, 0x64, 0, 0x1f, 0xac, 0x24, 0x84, 1, 0x40, 0x16, 0xec, 4,
		0x40, 0, 0, 3, 0, 0x40, 0, 0, 0x0c, 0x23, 0xc6, 0x0c, 0x92, 1,
		0, 5, 0x68, 0xee, 0x32, 0xc8, 0xb0,
	})
	audio := recorderPacket(t, packet.PktAudio, 0, []byte{0xaf, 0, 0x12, 0x10})
	require.NoError(t, writer.Write(video))
	require.NoError(t, writer.Write(audio))
}

func TestRecordingBoundarySwitchAndFailureIsolation(t *testing.T) {
	info := base.NewInfo(t.Name(), "sample", false)
	app := &config.App{Enable: true, AppName: info.App}
	config.Get().SetApp(info.App, app)
	writer := newSwitchingWriter(info, &base.ConnectInfo{})
	defer writer.After()
	seedRecorder(t, writer)
	keyframe := recorderPacket(t, packet.PktVideo, 2000, []byte{0x17, 1, 0, 0, 0, 0, 0, 0, 1, 0x65})
	interframe := recorderPacket(t, packet.PktVideo, 2040, []byte{0x27, 1, 0, 0, 0, 0, 0, 0, 1, 0x41})
	require.NoError(t, writer.Write(keyframe))
	require.Nil(t, writer.terminal)
	next := *app
	next.Recorder = config.AppRecorder{Enable: true, SaveAs: "flv", SaveMode: "immediate", Segment: 4,
		Driver: "fs", SaveTo: filepath.Join(t.TempDir(), "flv")}
	config.Get().SetApp(info.App, &next)
	require.NoError(t, writer.Write(interframe))
	require.Nil(t, writer.terminal)
	require.NoError(t, writer.Write(keyframe))
	require.IsType(t, &recordflv.Stream{}, writer.terminal)
	previous := writer.terminal
	changed := next
	changed.Recorder.SaveAs, changed.Recorder.SaveTo = "ts", filepath.Join(t.TempDir(), "ts")
	config.Get().SetApp(info.App, &changed)
	require.NoError(t, writer.Write(interframe))
	require.Same(t, previous, writer.terminal)
	keyframe.Baseline = 4000
	require.NoError(t, writer.Write(keyframe))
	require.IsType(t, &recordts.Stream{}, writer.terminal)
	files, err := filepath.Glob(filepath.Join(next.Recorder.SaveTo, info.Key, "*.flv"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	payload, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Greater(t, len(payload), 13)
	require.Equal(t, "FLV", string(payload[:3]))
	disabled := changed
	disabled.Recorder.Enable = false
	config.Get().SetApp(info.App, &disabled)
	require.NoError(t, writer.Write(keyframe))
	require.Nil(t, writer.terminal)
	files, err = filepath.Glob(filepath.Join(changed.Recorder.SaveTo, info.Key, "*.ts"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	payload, err = os.ReadFile(files[0])
	require.NoError(t, err)
	require.Greater(t, len(payload), 188)
	require.Equal(t, byte(0x47), payload[0])
	broken := changed
	broken.Recorder.SaveTo = filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(broken.Recorder.SaveTo, []byte("blocked"), 0600))
	config.Get().SetApp(info.App, &broken)
	require.NoError(t, writer.Write(keyframe), "recording failure must not propagate to live media")
	require.Nil(t, writer.terminal)
	var failed appstate.Status
	for _, state := range appstate.Snapshot() {
		if state.Component == "recording" && state.Stream == info.Key {
			failed = state
		}
	}
	require.Equal(t, "failed", failed.State)
	require.Equal(t, uint64(1), failed.Failures)
	repaired := changed
	repaired.Recorder.SaveTo = filepath.Join(t.TempDir(), "recovered")
	config.Get().SetApp(info.App, &repaired)
	require.NoError(t, writer.Write(keyframe))
	require.NotNil(t, writer.terminal)
	for _, state := range appstate.Snapshot() {
		if state.Component == "recording" && state.Stream == info.Key {
			require.Equal(t, "active", state.State)
			require.Equal(t, uint64(1), state.Failures, "successful recovery must not erase failure evidence")
		}
	}
}
