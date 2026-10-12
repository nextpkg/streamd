package record

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func TestUnchangedRecordingRefreshesFutureFragmentClients(t *testing.T) {
	server := teststore.New(t, "test-bucket")
	defer server.Close()
	filename := filepath.Join(t.TempDir(), "config.json")
	info := base.NewInfo(t.Name(), "sample", false)
	settings := config.NewConfig()
	settings.App = []config.App{{Enable: true, AppName: info.App, Recorder: config.AppRecorder{Enable: true, SaveAs: "flv", SaveMode: "immediate", Segment: 4, SaveTo: t.TempDir(), Driver: "aws",
		AwsCfg: &objectstore.S3Config{Enable: true, Endpoint: server.URL, Bucket: "test-bucket", SecretID: "test-id", SecretKey: "test-secret", PathStyle: true}}}}
	write := func() {
		body, err := json.Marshal(settings)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filename, body, 0600))
	}
	write()
	manager, err := config.NewManager(context.Background(), filename)
	require.NoError(t, err)
	defer func() { config.Get().Revoke() }()
	writer := newSwitchingWriter(info, &base.ConnectInfo{})
	defer writer.After()
	seedRecorder(t, writer)
	frame := recorderPacket(t, packet.PktVideo, 2000, []byte{0x17, 1, 0, 0, 0, 0, 0, 0, 1, 0x65})
	require.NoError(t, writer.Write(frame))
	terminal, store := writer.terminal, writer.store
	previous := config.Get().GetRecorder(info.App).AwsClient()
	require.Equal(t, 1, previous.InspectionStats().Pending)
	settings.LogLevel = "DEBUG"
	write()
	require.NoError(t, manager.Reload())
	next := config.Get().GetRecorder(info.App).AwsClient()
	require.NotSame(t, previous, next)
	require.Equal(t, 1, previous.InspectionStats().Pending, "active recorder owns its previous client until the keyframe handoff")
	frame.Baseline = 2040
	require.NoError(t, writer.Write(frame))
	require.Same(t, terminal, writer.terminal, "unchanged recording must keep its media terminal")
	require.Same(t, store, writer.store, "client refresh must not replace the active segment")
	require.NoError(t, store.Close(true))
	require.NoError(t, store.Open())
	require.Equal(t, 1, next.InspectionStats().Pending, "next fragment must schedule inspection on the current configuration client")
	require.Zero(t, previous.InspectionStats().Pending)
}
