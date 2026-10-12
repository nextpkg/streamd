package driver_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
	"github.com/nextpkg/streamd/record/storage/driver"
	"github.com/stretchr/testify/require"
)

func TestExpiredInspectionRetriesSavedTailAfterCompletionFailure(test *testing.T) {
	for _, fullParts := range []int{0, 1} {
		test.Run(fmt.Sprint(fullParts), func(test *testing.T) {
			eachCheckpointProvider(test, func(test *testing.T, provider string, server *teststore.Server) {
				client := checkpointClient(test, provider, server.URL)
				key := fmt.Sprintf("retry/%d/1.flv", fullParts)
				body := bytes.Repeat([]byte{0x67}, fullParts*objectstore.MinBlockSize+23)
				upload, err := client.NewMultipartUpload(context.Background(), key, true)
				require.NoError(test, err)
				if fullParts != 0 {
					require.NoError(test, upload.Upload(context.Background(), body[:objectstore.MinBlockSize]))
				}
				require.NoError(test, upload.Suspend(context.Background(), body[fullParts*objectstore.MinBlockSize:]))
				keys := server.ObjectKeys()
				require.Len(test, keys, 1)
				checkpointKey := keys[0]
				saved := server.Download(checkpointKey)
				server.Reject(http.MethodPost, key, http.StatusForbidden)
				result, err := client.InspectExpired(context.Background(), key, 900, driver.IsExpired)
				require.Error(test, err)
				require.Zero(test, result.Finalized)
				require.Equal(test, saved, server.Download(checkpointKey))
				require.Equal(test, 1, server.PendingUploads())
				_, err = client.NewMultipartUpload(context.Background(), key, true)
				require.Error(test, err, "ordinary append must remain strict about a provisional short part")
				result, err = client.InspectExpired(context.Background(), key, 900, driver.IsExpired)
				require.Error(test, err)
				require.Zero(test, result.Finalized)
				require.Equal(test, saved, server.Download(checkpointKey))
				server.Reject(http.MethodPost, key, 0)
				server.Reject(http.MethodPut, key, http.StatusForbidden)
				result, err = client.InspectExpired(context.Background(), key, 900, driver.IsExpired)
				require.ErrorContains(test, err, "upload part")
				require.Zero(test, result.Finalized)
				require.Equal(test, saved, server.Download(checkpointKey))
				require.Equal(test, 1, server.PendingUploads())
				server.Reject(http.MethodPut, key, 0)
				result, err = client.InspectExpired(context.Background(), key, 900, driver.IsExpired)
				require.NoError(test, err, "a failed completion must remain retryable from its saved tail")
				require.Equal(test, 1, result.Finalized)
				require.Equal(test, body, server.Download(key))
				require.Zero(test, server.PendingUploads())
				for _, candidate := range server.ObjectKeys() {
					require.NotEqual(test, checkpointKey, candidate)
				}
			})
		})
	}
}
