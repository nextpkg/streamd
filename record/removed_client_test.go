package record

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func TestRemovedApplicationMaintainsInspectionUntilRecordingEnds(test *testing.T) {
	server := teststore.New(test, "test-bucket")
	defer server.Close()
	filename := filepath.Join(test.TempDir(), "config.json")
	info := base.NewInfo(test.Name(), "sample", false)
	settings := config.NewConfig()
	settings.App = []config.App{{Enable: true, AppName: info.App, Recorder: config.AppRecorder{Enable: true, SaveAs: "flv", SaveMode: "immediate", Segment: 4, SaveTo: test.TempDir(), Driver: "aws",
		AwsCfg: &objectstore.S3Config{Enable: true, Endpoint: server.URL, Bucket: "test-bucket", SecretID: "test-id", SecretKey: "test-secret", PathStyle: true}}}}
	write := func() {
		body, err := json.Marshal(settings)
		require.NoError(test, err)
		require.NoError(test, os.WriteFile(filename, body, 0600))
	}
	write()
	manager, err := config.NewManager(context.Background(), filename)
	require.NoError(test, err)
	defer func() { config.Get().Revoke() }()
	writer := newSwitchingWriter(info, &base.ConnectInfo{})
	var stopped sync.Once
	stop := func() { stopped.Do(writer.After) }
	defer stop()
	seedRecorder(test, writer)
	frame := recorderPacket(test, packet.PktVideo, 2000, []byte{0x17, 1, 0, 0, 0, 0, 0, 0, 1, 0x65})
	require.NoError(test, writer.Write(frame))
	client := config.Get().GetRecorder(info.App).AwsClient()
	terminal, store := writer.terminal, writer.store
	require.Equal(test, 1, client.InspectionStats().Pending)
	settings.App = nil
	write()
	require.NoError(test, manager.Reload())
	require.Equal(test, 1, client.InspectionStats().Pending, "removing an application must not retire maintenance still owned by its live recording")
	frame.Baseline = 2040
	require.NoError(test, writer.Write(frame))
	require.Same(test, terminal, writer.terminal)
	require.Same(test, store, writer.store)
	require.Equal(test, "retained_for_existing_stream", recordingState(info).State)
	require.NoError(test, store.Close(true))
	require.NoError(test, store.Open())
	require.Equal(test, 1, client.InspectionStats().Pending)
	require.Positive(test, client.InspectionStats().Coalesced)
	stop()
	require.Zero(test, client.InspectionStats().Pending)
	require.Zero(test, client.InspectionStats().Running)
	require.Equal(test, 1, client.InspectionStats().Cancelled)
	require.False(test, client.ScheduleInspection("retired/1.flv", 4, func(string, int64) bool { return true }))
}
