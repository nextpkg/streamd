package objectstore

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/nextpkg/streamd/internal/mediatime"
	"github.com/stretchr/testify/require"
)

func TestMediaCheckpointBindsExtentFormatAndDigest(test *testing.T) {
	for _, scenario := range []struct {
		name   string
		modify func(*checkpoint)
	}{
		{"valid", func(*checkpoint) {}},
		{"wrong-extent", func(saved *checkpoint) { saved.Media.Bytes++ }},
		{"changed-time", func(saved *checkpoint) { saved.Media.Next++ }},
		{"changed-format", func(saved *checkpoint) { saved.Media.Format = "ts" }},
		{"missing-state", func(saved *checkpoint) { saved.Media = nil }},
		{"missing-digest", func(saved *checkpoint) { saved.MediaDigest = "" }},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			saved := validCheckpoint()
			saved.Media = &mediatime.State{Format: "flv", Bytes: partBytes(saved.Parts) + int64(len(saved.Tail)), Next: 200}
			saved.MediaDigest = mediaDigest(saved.Media)
			scenario.modify(&saved)
			data, err := json.Marshal(saved)
			require.NoError(test, err)
			client := &Client{Backend: &checkpointStub{saved: data}}
			loaded, err := client.loadCheckpoint(context.Background(), saved.Key)
			if scenario.name == "valid" {
				require.NoError(test, err)
				require.Equal(test, saved.Media, loaded.Media)
			} else {
				require.Error(test, err)
			}
		})
	}
}
