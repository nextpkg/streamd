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

func TestCredentialRotationPreservesActiveRecordingUpload(test *testing.T) {
	for _, provider := range []string{"aws", "cos"} {
		test.Run(provider, func(test *testing.T) {
			bucket := ""
			if provider == "aws" {
				bucket = "media"
			}
			server := teststore.New(test, bucket)
			filename := filepath.Join(test.TempDir(), "config.json")
			info := base.NewInfo("credentials-"+provider, "sample", false)
			settings := config.NewConfig()
			settings.App = []config.App{{Enable: true, AppName: info.App, Recorder: config.AppRecorder{Enable: true, SaveAs: "flv", SaveMode: "immediate", Segment: 3600, SaveTo: test.TempDir(), Driver: provider,
				AwsCfg: &objectstore.S3Config{Enable: true, Endpoint: server.URL, Bucket: bucket, SecretID: "old-id", SecretKey: "old-secret", PathStyle: true},
				CosCfg: &objectstore.COSConfig{Enable: true, Endpoint: server.URL, SecretID: "old-id", SecretKey: "old-secret"}}}}
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
			defer writer.After()
			seedRecorder(test, writer)
			frame := recorderPacket(test, packet.PktVideo, 2000, []byte{0x17, 1, 0, 0, 0, 0, 0, 0, 1, 0x65})
			require.NoError(test, writer.Write(frame))
			terminal, storage := writer.terminal, writer.store
			client := config.Get().GetRecorder(info.App).AwsClient()
			if provider == "cos" {
				client = config.Get().GetRecorder(info.App).CosClient()
			}
			before, err := client.Backend.Uploads(context.Background(), info.Key+"/")
			require.NoError(test, err)
			require.Len(test, before, 1)
			settings.App[0].Recorder.AwsCfg.SecretID, settings.App[0].Recorder.AwsCfg.SecretKey = "new-id", "new-secret"
			settings.App[0].Recorder.CosCfg.SecretID, settings.App[0].Recorder.CosCfg.SecretKey = "new-id", "new-secret"
			write()
			require.NoError(test, manager.Reload())
			frame.Baseline = 4000
			require.NoError(test, writer.Write(frame))
			require.Same(test, terminal, writer.terminal, "credentials must not force a new media terminal or finalize a round bucket")
			require.Same(test, storage, writer.store)
			after, err := client.Backend.Uploads(context.Background(), info.Key+"/")
			require.NoError(test, err)
			require.Equal(test, before, after)
			require.Empty(test, server.ObjectKeys(), "current upload must not be prematurely completed")
			require.Equal(test, "new-id", writer.settings.AwsCfg.SecretID)
		})
	}
}
