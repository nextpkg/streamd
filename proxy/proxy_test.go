package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

import "github.com/nextpkg/streamd/config"

func proxyConfiguration(test *testing.T, settings config.Proxy) {
	test.Helper()
	previous, err := json.Marshal(config.Get())
	if err != nil {
		test.Fatal(err)
	}
	filename := filepath.Join(test.TempDir(), "proxy.json")
	encoded, err := json.Marshal(map[string]any{"proxy": settings})
	if err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(filename, encoded, 0600); err != nil {
		test.Fatal(err)
	}
	if _, err := config.NewManager(context.Background(), filename); err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() {
		if err := os.WriteFile(filename, previous, 0600); err != nil {
			test.Error(err)
			return
		}
		if _, err := config.NewManager(context.Background(), filename); err != nil {
			test.Error(err)
		}
	})
}

func proxyUpstream(test *testing.T, handler http.Handler) *httptest.Server {
	test.Helper()
	upstream := httptest.NewServer(handler)
	test.Cleanup(upstream.Close)
	previous := http.DefaultTransport
	transport := previous.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(upstream.URL, "http://"))
	}
	http.DefaultTransport = transport
	test.Cleanup(func() { http.DefaultTransport = previous; transport.CloseIdleConnections() })
	return upstream
}

func TestProxyRejectsMismatchedMediaRoutes(test *testing.T) {
	proxyConfiguration(test, config.Proxy{Prefix: "edge"})
	var requests atomic.Int64
	proxyUpstream(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = response.Write([]byte("media"))
	}))
	config.GetFinder().Register("live", "sample")
	router := Init()
	test.Cleanup(router.Close)
	for _, location := range []string{"/edge/hfs/live/sample.m3u8", "/edge/hls/live/sample.flv", "/edge/hls/live/sample/.ts", "/edge/hfs/live/.flv", "/edge/hls/live/.m3u8"} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, location, nil))
		if response.Code != http.StatusNotFound {
			test.Errorf("route %s returned %d, expected 404", location, response.Code)
		}
	}
	if requests.Load() != 0 {
		test.Fatalf("invalid routes reached the upstream %d times", requests.Load())
	}
}

func TestProxyMissingSourcePreservesCallbackQueryValues(test *testing.T) {
	queries := make(chan url.Values, 1)
	callback := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		queries <- request.URL.Query()
		response.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(response, "missing-source-body")
	}))
	defer callback.Close()
	proxyConfiguration(test, config.Proxy{Prefix: "edge", OnNotFound: callback.URL + "?app=configured&instance=fallback&keep=%2B"})
	router := Init()
	test.Cleanup(router.Close)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/edge/hfs/live/absent.flv", nil))
	if response.Code != http.StatusNotFound || response.Body.String() != "missing-source-body" {
		test.Fatalf("callback changed not-found response: %d %q", response.Code, response.Body.String())
	}
	query := <-queries
	if !reflect.DeepEqual(query["app"], []string{"configured", "live"}) || !reflect.DeepEqual(query["instance"], []string{"fallback", "absent"}) || query.Get("keep") != "+" {
		test.Fatalf("callback replaced the configured query values: %v", query)
	}
}

func TestProxyExposesRequestOwnerForShutdown(test *testing.T) {
	router := Init()
	owner, ok := any(router).(interface{ Close() })
	if !ok {
		test.Fatal("proxy router exposes no owner to cancel and join active requests")
	}
	owner.Close()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/live/hfs/live/sample.flv", nil))
	if response.Code != http.StatusServiceUnavailable {
		test.Fatalf("closed proxy still accepts requests: %d", response.Code)
	}
}
