package driver_test

import (
	"bytes"
	"testing"

	"github.com/nextpkg/streamd/internal/mediatime"
	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
	"github.com/nextpkg/streamd/record/storage/driver"
	"github.com/stretchr/testify/require"
)

func TestObjectMediaStateSurvivesMultipartClientReplacement(test *testing.T) {
	eachCheckpointProvider(test, func(test *testing.T, provider string, server *teststore.Server) {
		key := "media/2000000000.ts"
		var expected []byte
		for session, size := range []int{objectstore.MinBlockSize + 7, objectstore.MinBlockSize + 3, 1 << 20} {
			writer := driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
			require.NoError(test, writer.Open())
			media := writer.(interface {
				WriteMedia([]byte, mediatime.State) error
				ResumeMedia() (*mediatime.State, int64)
			})
			state, extent := media.ResumeMedia()
			require.Equal(test, int64(len(expected)), extent)
			if session == 0 {
				require.Nil(test, state)
			} else {
				require.Equal(test, &mediatime.State{Format: "ts", Bytes: extent, Next: uint64(session * 1000)}, state)
				state.Next = 999999
				unchanged, _ := media.ResumeMedia()
				require.Equal(test, uint64(session*1000), unchanged.Next)
			}
			data := bytes.Repeat([]byte{byte(session + 1)}, size)
			expected = append(expected, data...)
			require.NoError(test, media.WriteMedia(data, mediatime.State{Format: "ts", Next: uint64((session + 1) * 1000)}))
			require.NoError(test, writer.Close(session == 2, true))
		}
		require.True(test, bytes.Equal(expected, server.Download(key)), "media state altered recording bytes")
		require.Equal(test, []string{key}, server.ObjectKeys())
		require.Zero(test, server.PendingUploads())
	})
}
