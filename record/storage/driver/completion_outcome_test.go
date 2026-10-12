package driver_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
	"github.com/nextpkg/streamd/record/storage/driver"
	"github.com/stretchr/testify/require"
)

func TestCompletedObjectSurvivesAmbiguousResultAndFreshClients(test *testing.T) {
	eachCheckpointProvider(test, func(test *testing.T, provider string, server *teststore.Server) {
		for _, mode := range []string{"response-lost", "cleanup-denied"} {
			test.Run(mode, func(test *testing.T) {
				bucket := ""
				if provider == "s3" {
					bucket = "bucket"
				}
				fault, err := teststore.NewCompletionFault(server.URL, bucket, mode)
				require.NoError(test, err)
				defer fault.Close()
				key := mode + "/sample/2000000000.flv"
				prefix := bytes.Repeat([]byte{0x61}, objectstore.MinBlockSize+17)
				writer := driver.NewObject(checkpointClient(test, provider, fault.URL), key, 900, true)
				require.NoError(test, writer.Open())
				require.NoError(test, writer.Write(prefix))
				require.NoError(test, writer.Close(false, true))
				var checkpointKey string
				for _, candidate := range server.ObjectKeys() {
					if len(candidate) > len(mode) && candidate[:len(mode)] == mode {
						checkpointKey = candidate
					}
				}
				require.NotEmpty(test, checkpointKey)
				saved := server.Download(checkpointKey)
				writer = driver.NewObject(checkpointClient(test, provider, fault.URL), key, 900, true)
				require.NoError(test, writer.Open())
				suffix := []byte("resumed-before-ambiguous-completion")
				require.NoError(test, writer.Write(suffix))
				require.NoError(test, fault.Arm([]string{key}))
				err = writer.Close(true, true)
				require.Error(test, err)
				if mode == "cleanup-denied" {
					require.ErrorContains(test, err, "remove completed recording checkpoint")
				}
				require.Error(test, writer.Close(true, true))
				expected := append(prefix, suffix...)
				require.Equal(test, expected, server.Download(key))
				require.Equal(test, saved, server.Download(checkpointKey))
				fresh := checkpointClient(test, provider, fault.URL)
				for attempt := 0; attempt < 2; attempt++ {
					reopened := driver.NewObject(fresh, key, 900, true)
					require.ErrorContains(test, reopened.Open(), "missing multipart upload")
				}
				result, err := fresh.InspectExpired(context.Background(), key, 900, func(string, int64) bool { return true })
				require.NoError(test, err)
				require.Zero(test, result.Finalized)
				require.Zero(test, server.PendingUploads())
				require.Equal(test, expected, server.Download(key))
				require.Equal(test, saved, server.Download(checkpointKey))
				outcomes, failures := fault.Snapshot()
				require.Empty(test, failures)
				outcome := outcomes[key]
				require.Equal(test, 1, outcome.Created)
				require.Equal(test, 1, outcome.SuccessfulCompletions)
				require.Len(test, outcome.CompletionManifestSHA256, 64)
				require.Zero(test, outcome.Aborted)
				require.Len(test, outcome.Parts, 2)
				require.NoError(test, teststore.VerifyCompletedParts(server.Download(key), outcome.Parts))
				if mode == "response-lost" {
					require.Equal(test, 1, outcome.Dropped)
					require.Zero(test, outcome.Denied)
				} else {
					require.Equal(test, 1, outcome.Denied)
					require.Zero(test, outcome.Dropped)
				}
			})
		}
	})
}
