package driver_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
	"github.com/nextpkg/streamd/record/storage/driver"
	"github.com/stretchr/testify/require"
)

func TestMultipartPartQueriesStayOffTheUploadPath(test *testing.T) {
	eachCheckpointProvider(test, func(test *testing.T, provider string, server *teststore.Server) {
		target, err := url.Parse(server.URL)
		require.NoError(test, err)
		proxy := httputil.NewSingleHostReverseProxy(target)
		var partQueries atomic.Int64
		observer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if request.Method == http.MethodGet && request.URL.Query().Get("uploadId") != "" {
				partQueries.Add(1)
			}
			proxy.ServeHTTP(response, request)
		}))
		defer observer.Close()
		key := "queries/sample/2000000000.flv"
		first := bytes.Repeat([]byte{0x71}, 2*objectstore.MinBlockSize+17)
		second := bytes.Repeat([]byte{0x82}, objectstore.MinBlockSize+23)
		client := checkpointClient(test, provider, observer.URL)
		writer := driver.NewObject(client, key, 900, true)
		require.NoError(test, writer.Open())
		require.NoError(test, writer.Write(first))
		require.NoError(test, writer.Close(false, true))
		require.Zero(test, partQueries.Load(), "new parts caused ListParts calls")
		client.Close()
		client = checkpointClient(test, provider, observer.URL)
		writer = driver.NewObject(client, key, 900, true)
		require.NoError(test, writer.Open())
		require.Equal(test, int64(1), partQueries.Load(), "resumption did not validate its remote manifest once")
		require.NoError(test, writer.Write(second))
		require.NoError(test, writer.Close(true, true))
		require.Equal(test, int64(1), partQueries.Load(), "append/complete repeatedly queried ListParts")
		require.True(test, bytes.Equal(append(first, second...), server.Download(key)), "manifest optimization changed recording bytes")
	})
}
