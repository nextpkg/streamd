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

func checkpointClient(test *testing.T, provider, endpoint string) *objectstore.Client {
	test.Helper()
	var client *objectstore.Client
	var err error
	if provider == "s3" {
		client, err = objectstore.NewS3(&objectstore.S3Config{Enable: true, Endpoint: endpoint, Bucket: "bucket", SecretID: "test-id", SecretKey: "test-secret", PathStyle: true})
	} else {
		client, err = objectstore.NewCOS(&objectstore.COSConfig{Enable: true, Endpoint: endpoint, SecretID: "test-id", SecretKey: "test-secret"})
	}
	require.NoError(test, err)
	test.Cleanup(client.Close)
	return client
}

func TestObjectPauseRetainsShortTailAcrossClients(test *testing.T) {
	for _, provider := range []string{"s3", "cos"} {
		test.Run(provider, func(test *testing.T) {
			bucket := ""
			if provider == "s3" {
				bucket = "bucket"
			}
			server := teststore.New(test, bucket)
			key := "test/sample/2000000000.flv"
			var expected []byte
			for session, size := range []int{1 << 20, objectstore.MinBlockSize + 7, 2 << 20} {
				client := checkpointClient(test, provider, server.URL)
				writer := driver.NewObject(client, key, 900, true)
				require.NoError(test, writer.Open())
				data := bytes.Repeat([]byte{byte(session + 1)}, size)
				expected = append(expected, data...)
				require.NoError(test, writer.Write(data))
				require.NoError(test, writer.Close(session == 2, true))
				client.Close()
			}
			actual := server.Download(key)
			require.True(test, bytes.Equal(expected, actual), "payload changed: got %d bytes, expected %d", len(actual), len(expected))
			require.Equal(test, []string{key}, server.ObjectKeys())
			require.Zero(test, server.PendingUploads())
		})
	}
}

func eachCheckpointProvider(test *testing.T, check func(*testing.T, string, *teststore.Server)) {
	test.Helper()
	for _, provider := range []string{"s3", "cos"} {
		test.Run(provider, func(test *testing.T) {
			bucket := ""
			if provider == "s3" {
				bucket = "bucket"
			}
			check(test, provider, teststore.New(test, bucket))
		})
	}
}

func TestObjectPauseMultipartBoundaries(test *testing.T) {
	eachCheckpointProvider(test, func(test *testing.T, provider string, server *teststore.Server) {
		for _, size := range []int{0, objectstore.MinBlockSize - 1, objectstore.MinBlockSize, objectstore.MinBlockSize + 1} {
			test.Run(fmt.Sprint(size), func(test *testing.T) {
				key := fmt.Sprintf("boundary/%d/2000000000.ts", size)
				data := bytes.Repeat([]byte{0x47}, size)
				client := checkpointClient(test, provider, server.URL)
				writer := driver.NewObject(client, key, 900, true)
				require.NoError(test, writer.Open())
				require.NoError(test, writer.Write(data))
				require.NoError(test, writer.Close(false, false))
				require.NoError(test, writer.Close(false, true))
				client.Close()
				resumed := driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
				require.NoError(test, resumed.Open())
				suffix := []byte("continuation")
				require.NoError(test, resumed.Write(suffix))
				require.NoError(test, resumed.Close(true, true))
				require.True(test, bytes.Equal(append(data, suffix...), server.Download(key)), "boundary payload differs")
				require.Zero(test, server.PendingUploads())
			})
		}
		require.Len(test, server.ObjectKeys(), 4)
	})
}

func TestObjectExpiredUploadIncludesSavedTail(test *testing.T) {
	eachCheckpointProvider(test, func(test *testing.T, provider string, server *teststore.Server) {
		oldKey, newKey := "expired/1.flv", "expired/2000000000.flv"
		data := bytes.Repeat([]byte{0x57}, objectstore.MinBlockSize+31)
		writer := driver.NewObject(checkpointClient(test, provider, server.URL), oldKey, 900, true)
		require.NoError(test, writer.Open())
		require.NoError(test, writer.Write(data))
		require.NoError(test, writer.Close(false, true))
		client := checkpointClient(test, provider, server.URL)
		writer = driver.NewObject(client, newKey, 900, true)
		require.NoError(test, writer.Open())
		result, err := client.InspectExpired(context.Background(), newKey, 900, driver.IsExpired)
		require.NoError(test, err)
		require.Equal(test, 1, result.Finalized)
		require.True(test, bytes.Equal(data, server.Download(oldKey)), "expired upload lost its saved tail")
		require.NoError(test, writer.Close(true, true))
		require.Equal(test, []string{oldKey}, server.ObjectKeys())
		require.Zero(test, server.PendingUploads())
	})
}

func TestObjectCheckpointFaultsPreserveRemoteProgress(test *testing.T) {
	eachCheckpointProvider(test, func(test *testing.T, provider string, server *teststore.Server) {
		key := "fault/2000000000.flv"
		data := bytes.Repeat([]byte{0x61}, objectstore.MinBlockSize+15)
		writer := driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.NoError(test, writer.Open())
		require.NoError(test, writer.Write(data))
		require.NoError(test, writer.Close(false, true))
		keys := server.ObjectKeys()
		require.Len(test, keys, 1)
		checkpointKey := keys[0]
		saved := server.Download(checkpointKey)
		server.Reject(http.MethodGet, checkpointKey, http.StatusForbidden)
		writer = driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.ErrorContains(test, writer.Open(), "read recording checkpoint")
		require.Equal(test, 1, server.PendingUploads())
		server.Reject(http.MethodGet, checkpointKey, 0)
		server.Store(checkpointKey, []byte("corrupt"))
		writer = driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.ErrorContains(test, writer.Open(), "decode recording checkpoint")
		require.Equal(test, 1, server.PendingUploads())
		server.Store(checkpointKey, saved)
		writer = driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.NoError(test, writer.Open())
		server.Reject(http.MethodPut, checkpointKey, http.StatusForbidden)
		require.ErrorContains(test, writer.Close(false, true), "save recording checkpoint")
		require.True(test, bytes.Equal(saved, server.Download(checkpointKey)), "failed save altered checkpoint")
		require.Equal(test, 1, server.PendingUploads())
		server.Reject(http.MethodPut, checkpointKey, 0)
		writer = driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.NoError(test, writer.Open())
		server.Reject(http.MethodDelete, checkpointKey, http.StatusForbidden)
		require.ErrorContains(test, writer.Close(true, true), "remove completed recording checkpoint")
		require.True(test, bytes.Equal(data, server.Download(key)), "completed object altered by checkpoint deletion failure")
		require.Zero(test, server.PendingUploads())
		writer = driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.ErrorContains(test, writer.Open(), "missing multipart upload")
		require.Len(test, server.ObjectKeys(), 2)
	})
}

func TestObjectUploadFailureKeepsConfirmedCheckpoint(test *testing.T) {
	eachCheckpointProvider(test, func(test *testing.T, provider string, server *teststore.Server) {
		key := "upload-failure/2000000000.flv"
		initial := bytes.Repeat([]byte{0x31}, 1<<20)
		writer := driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.NoError(test, writer.Open())
		require.NoError(test, writer.Write(initial))
		require.NoError(test, writer.Close(false, true))
		keys := server.ObjectKeys()
		require.Len(test, keys, 1)
		checkpointKey := keys[0]
		saved := server.Download(checkpointKey)
		writer = driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.NoError(test, writer.Open())
		server.Reject(http.MethodPut, key, http.StatusForbidden)
		continuation := bytes.Repeat([]byte{0x32}, objectstore.MinBlockSize)
		require.NoError(test, writer.Write(continuation))
		require.ErrorContains(test, writer.Close(false, true), "upload part 1")
		require.Equal(test, 1, server.PendingUploads())
		require.True(test, bytes.Equal(saved, server.Download(checkpointKey)), "upload failure changed confirmed checkpoint")
		server.Reject(http.MethodPut, key, 0)
		writer = driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.NoError(test, writer.Open())
		require.NoError(test, writer.Write(continuation))
		require.NoError(test, writer.Close(true, true))
		require.True(test, bytes.Equal(append(initial, continuation...), server.Download(key)), "retry did not preserve confirmed prefix exactly")
		require.Equal(test, []string{key}, server.ObjectKeys())
		require.Zero(test, server.PendingUploads())
	})
}

func TestObjectNewPartAndFailedCheckpointRejectUnsafeResume(test *testing.T) {
	eachCheckpointProvider(test, func(test *testing.T, provider string, server *teststore.Server) {
		key := "stale/2000000000.ts"
		writer := driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.NoError(test, writer.Open())
		require.NoError(test, writer.Write(bytes.Repeat([]byte{0x47}, objectstore.MinBlockSize+9)))
		require.NoError(test, writer.Close(false, true))
		keys := server.ObjectKeys()
		require.Len(test, keys, 1)
		checkpointKey := keys[0]
		saved := server.Download(checkpointKey)
		writer = driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.NoError(test, writer.Open())
		require.NoError(test, writer.Write(bytes.Repeat([]byte{0x48}, objectstore.MinBlockSize)))
		server.Reject(http.MethodPut, checkpointKey, http.StatusForbidden)
		require.ErrorContains(test, writer.Close(false, true), "save recording checkpoint")
		require.True(test, bytes.Equal(saved, server.Download(checkpointKey)), "failed checkpoint altered previous state")
		server.Reject(http.MethodPut, checkpointKey, 0)
		writer = driver.NewObject(checkpointClient(test, provider, server.URL), key, 900, true)
		require.ErrorContains(test, writer.Open(), "does not match remote upload")
		require.Equal(test, 1, server.PendingUploads())
		require.Equal(test, keys, server.ObjectKeys())
	})
}
