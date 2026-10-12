package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nextpkg/streamd/config"
)

func TestProxyUsesHTTPAuthorityWithoutDialingRTMPPort(test *testing.T) {
	var rtmpConnections atomic.Int64
	rtmpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			connection, err := rtmpListener.Accept()
			if err != nil {
				return
			}
			rtmpConnections.Add(1)
			_ = connection.Close()
		}
	}()
	defer func() { _ = rtmpListener.Close(); <-finished }()
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(response, request.URL.RequestURI())
	}))
	defer upstream.Close()
	proxyConfiguration(test, config.Proxy{Prefix: "via"})
	filename := filepath.Join(test.TempDir(), "service.json")
	body, err := json.Marshal(map[string]any{"proxy": map[string]string{"prefix": "via"}, "finder": map[string]string{"advice": rtmpListener.Addr().String(), "http_advice": strings.TrimPrefix(upstream.URL, "http://")}})
	if err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(filename, body, 0600); err != nil {
		test.Fatal(err)
	}
	if _, err := config.NewManager(context.Background(), filename); err != nil {
		test.Fatal(err)
	}
	config.GetFinder().Register("live", "sample")
	router := Init()
	defer router.Close()
	server := httptest.NewServer(router)
	defer server.Close()
	client := proxyClient(test)
	for _, route := range []string{"/hfs/live/sample.flv", "/hls/live/sample.m3u8", "/hls/live/sample/1.ts"} {
		response, err := client.Get(server.URL + "/via" + route + "?part=one")
		if err != nil {
			test.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || string(body) != route+"?part=one" {
			test.Fatalf("proxy did not use HTTP endpoint: status=%d body=%q err=%v", response.StatusCode, body, err)
		}
	}
	if rtmpConnections.Load() != 0 {
		test.Fatalf("HTTP requests reached the RTMP port: %d", rtmpConnections.Load())
	}
}
