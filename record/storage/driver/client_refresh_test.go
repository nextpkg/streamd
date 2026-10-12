package driver_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
	"github.com/nextpkg/streamd/record/storage/driver"
	"github.com/stretchr/testify/require"
)

func TestRecordingClientsRefreshAddressesWithoutReplacingActiveUploads(test *testing.T) {
	for _, provider := range []string{"aws", "cos"} {
		test.Run(provider, func(test *testing.T) {
			bucket := ""
			if provider == "aws" {
				bucket = "test-bucket"
			}
			store := teststore.New(test, bucket)
			defer store.Close()
			var route atomic.Int32
			var lock sync.Mutex
			seen := [2]map[string]int{make(map[string]int), make(map[string]int)}
			servers := make([]*httptest.Server, 2)
			for index := range servers {
				servers[index] = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
					lock.Lock()
					seen[index][strings.TrimPrefix(request.URL.Path, "/"+bucket+"/")]++
					lock.Unlock()
					store.Config.Handler.ServeHTTP(response, request)
				}))
				defer servers[index].Close()
			}
			original := http.DefaultTransport
			transport := original.(*http.Transport).Clone()
			transport.Proxy = nil
			transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				var dialer net.Dialer
				if !strings.HasPrefix(address, "recording-refresh.invalid:") {
					return dialer.DialContext(ctx, network, address)
				}
				return dialer.DialContext(ctx, network, servers[route.Load()].Listener.Addr().String())
			}
			http.DefaultTransport = transport
			defer func() { http.DefaultTransport = original; transport.CloseIdleConnections() }()
			var client *objectstore.Client
			var err error
			if provider == "aws" {
				client, err = objectstore.NewS3(&objectstore.S3Config{Enable: true, Endpoint: "http://recording-refresh.invalid", Bucket: bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: true})
			} else {
				client, err = objectstore.NewCOS(&objectstore.COSConfig{Enable: true, Endpoint: "http://recording-refresh.invalid", SecretID: "test-id", SecretKey: "test-secret"})
			}
			require.NoError(test, err)
			defer client.Close()
			first := driver.NewObject(client, "refresh/first.flv", 900, true)
			require.NoError(test, first.Open())
			defer first.Close(true, true)
			require.NoError(test, first.Write([]byte("active-before-address-change")))
			route.Store(1)
			second := driver.NewObject(client, "refresh/second.flv", 900, true)
			require.NoError(test, second.Open())
			defer second.Close(true, true)
			require.NoError(test, second.Write([]byte("new-address")))
			require.NoError(test, second.Close(true, true))
			require.NoError(test, first.Write([]byte("-continued")))
			require.NoError(test, first.Close(true, true))
			require.Equal(test, []byte("active-before-address-change-continued"), store.Download("refresh/first.flv"))
			require.Equal(test, []byte("new-address"), store.Download("refresh/second.flv"))
			firstKey, secondKey := "refresh/first.flv", "refresh/second.flv"
			if provider == "cos" {
				firstKey, secondKey = "/"+firstKey, "/"+secondKey
			}
			lock.Lock()
			defer lock.Unlock()
			require.Positive(test, seen[0][firstKey], "active upload did not use its original route")
			require.Zero(test, seen[1][firstKey], "new fragment interrupted the active upload's connection")
			require.Zero(test, seen[0][secondKey], "new recording reused the stale endpoint connection")
			require.Positive(test, seen[1][secondKey], "new recording did not re-resolve its address")
		})
	}
}
