package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/hls"
)

func prefixManager(test *testing.T, settings config.Proxy) (*config.Manager, func(config.Proxy) error) {
	test.Helper()
	proxyConfiguration(test, settings)
	filename := filepath.Join(test.TempDir(), "reload.json")
	write := func(next config.Proxy) error {
		body, err := json.Marshal(map[string]any{"proxy": next})
		if err != nil {
			return err
		}
		return os.WriteFile(filename, body, 0600)
	}
	if err := write(settings); err != nil {
		test.Fatal(err)
	}
	manager, err := config.NewManager(context.Background(), filename)
	if err != nil {
		test.Fatal(err)
	}
	return manager, func(next config.Proxy) error {
		if err := write(next); err != nil {
			return err
		}
		return manager.Reload()
	}
}

func TestProxyIssuedPlaylistAndSegmentsSurvivePrefixReloads(test *testing.T) {
	manager, reload := prefixManager(test, config.Proxy{Prefix: "before"})
	proxyUpstream(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, ".m3u8") {
			_, _ = io.WriteString(response, "#EXTM3U\n#EXTINF:2,\nsample/1.ts?part=original\n")
			return
		}
		_, _ = io.WriteString(response, request.URL.RequestURI())
	}))
	config.GetFinder().Register("live", "sample")
	router := Init()
	defer router.Close()
	server := httptest.NewServer(router)
	defer server.Close()
	client := proxyClient(test)
	check := func(prefix string) {
		test.Helper()
		for _, route := range []string{"/hls/live/sample.m3u8", "/hls/live/sample/1.ts?part=original", "/hfs/live/sample.flv"} {
			response, err := client.Get(server.URL + prefix + route)
			if err != nil {
				test.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil || response.StatusCode != http.StatusOK {
				test.Fatalf("issued route %s%s failed: status=%d body=%q err=%v", prefix, route, response.StatusCode, body, err)
			}
			if !strings.HasSuffix(route, ".m3u8") && string(body) != route {
				test.Fatalf("upstream route/query changed: %q", body)
			}
		}
	}
	check("/before")
	prefixes := []string{"/before"}
	for _, prefix := range []string{"after", "after/nested", "before", "final"} {
		if err := reload(config.Proxy{Prefix: prefix}); err != nil {
			test.Fatal(err)
		}
		prefixes = append(prefixes, "/"+prefix)
		for _, issued := range prefixes {
			check(issued)
		}
	}
	if manager.Version() != 5 {
		test.Fatalf("reloads did not commit: %d", manager.Version())
	}
}

func TestProxyAliasUsesCurrentPolicyAndRejectsUncommittedPrefixes(test *testing.T) {
	var oldCallbacks, newCallbacks atomic.Int64
	upstream := proxyUpstream(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/old-callback":
			oldCallbacks.Add(1)
		case "/new-callback":
			newCallbacks.Add(1)
		}
		_, _ = io.WriteString(response, request.URL.Path)
	}))
	manager, reload := prefixManager(test, config.Proxy{Prefix: "before", OnNotFound: upstream.URL + "/old-callback"})
	router := Init()
	defer router.Close()
	settings := config.Proxy{Prefix: "after", EnableSecure: true, SecureKey: "current-secret", SecureTTL: 60, OnNotFound: upstream.URL + "/new-callback"}
	if err := reload(settings); err != nil {
		test.Fatal(err)
	}
	config.GetFinder().Register("live", "newly-registered")
	expiration := time.Now().Unix() + 30
	query := func(instance string) string {
		return "?token=" + config.AntiLeechSign("live/"+instance, settings.SecureKey, expiration) + "&e=" + strconv.FormatInt(expiration, 10)
	}
	for _, prefix := range []string{"before", "after"} {
		for _, scenario := range []struct {
			path   string
			status int
			body   string
		}{
			{"newly-registered.flv", http.StatusForbidden, ""},
			{"newly-registered.flv" + query("newly-registered"), http.StatusOK, "/hfs/live/newly-registered.flv"},
			{"absent.flv" + query("absent"), http.StatusNotFound, "/new-callback"},
		} {
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/"+prefix+"/hfs/live/"+scenario.path, nil))
			if response.Code != scenario.status || response.Body.String() != scenario.body {
				test.Fatalf("alias used stale policy: %s %s status=%d body=%q", prefix, scenario.path, response.Code, response.Body.String())
			}
		}
	}
	if oldCallbacks.Load() != 0 || newCallbacks.Load() != 2 {
		test.Fatalf("callback snapshot mismatch: old=%d new=%d", oldCallbacks.Load(), newCallbacks.Load())
	}
	previous := config.Get()
	settings.Prefix, settings.SecureTTL = "never-committed", 0
	if err := reload(settings); err == nil || config.Get() != previous || manager.Version() != 2 {
		test.Fatal("invalid reload changed published configuration")
	}
	for _, prefix := range []string{"never-committed", "unknown", "live"} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/"+prefix+"/hfs/live/absent.flv"+query("absent"), nil))
		if response.Code != http.StatusNotFound || response.Body.String() == "/new-callback" {
			test.Fatalf("uncommitted prefix reached callback: %s %d %q", prefix, response.Code, response.Body.String())
		}
	}
	router.Close()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/before/hfs/live/newly-registered.flv"+query("newly-registered"), nil))
	if response.Code != http.StatusServiceUnavailable {
		test.Fatalf("closed alias accepted a request: %d", response.Code)
	}
}

func TestProxyAliasPreservesEscapedSegmentsAndNestedPrefixes(test *testing.T) {
	_, reload := prefixManager(test, config.Proxy{Prefix: "edge"})
	proxyUpstream(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(response, request.URL.RequestURI())
	}))
	config.GetFinder().Register("live", "nested/channel")
	router := Init()
	defer router.Close()
	server := httptest.NewServer(router)
	defer server.Close()
	for _, prefix := range []string{"edge/hls", "café/media"} {
		if err := reload(config.Proxy{Prefix: prefix}); err != nil {
			test.Fatal(err)
		}
	}
	client := proxyClient(test)
	for _, prefix := range []string{"/edge", "/edge/hls", "/café/media"} {
		escapedPrefix := (&url.URL{Path: prefix}).EscapedPath()
		for _, route := range []string{"/hfs/live/nested%2Fchannel.flv", "/hls/live/nested%2Fchannel.m3u8", "/hls/live/nested%2Fchannel/1%2Ets"} {
			location := route + "?part=a%2Bb&part=next"
			response, err := client.Get(server.URL + escapedPrefix + location)
			if err != nil {
				test.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil || response.StatusCode != http.StatusOK || string(body) != location {
				test.Fatalf("escaped alias was not preserved: %s status=%d body=%q err=%v escaped=%q raw=%q path=%q", prefix+location, response.StatusCode, body, err, response.Request.URL.EscapedPath(), response.Request.URL.RawPath, response.Request.URL.Path)
			}
		}
	}
}

func TestProxyPrefixReloadDoesNotRaceWithIssuedRequests(test *testing.T) {
	_, reload := prefixManager(test, config.Proxy{Prefix: "before"})
	proxyUpstream(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(response, "segment")
	}))
	config.GetFinder().Register("live", "sample")
	router := Init()
	defer router.Close()
	server := httptest.NewServer(router)
	defer server.Close()
	client := proxyClient(test)
	failures := make(chan error, 8)
	var readers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for request := 0; request < 50; request++ {
				response, err := client.Get(server.URL + "/before/hls/live/sample/1.ts")
				if err != nil {
					failures <- err
					return
				}
				body, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if err != nil || response.StatusCode != http.StatusOK || string(body) != "segment" {
					failures <- fmt.Errorf("in-flight alias failed: status=%d body=%q err=%v", response.StatusCode, body, err)
					return
				}
			}
		}()
	}
	for revision := 0; revision < 20; revision++ {
		if err := reload(config.Proxy{Prefix: fmt.Sprintf("after/%d", revision)}); err != nil {
			test.Error(err)
			break
		}
	}
	readers.Wait()
	close(failures)
	for err := range failures {
		test.Error(err)
	}
}

func TestProxyPrefixDoesNotCollideWithLocalHLSRoutes(test *testing.T) {
	_, reload := prefixManager(test, config.Proxy{Prefix: "before"})
	proxyUpstream(test, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(response, "proxied:"+request.URL.Path)
	}))
	config.GetFinder().Register("live", "sample")
	local := &hls.Server{}
	defer local.Close()
	router := Init(local)
	defer router.Close()
	if err := reload(config.Proxy{Prefix: "hls"}); err != nil {
		test.Fatal(err)
	}
	for _, route := range []string{"/hfs/live/sample.flv", "/hls/live/sample.m3u8", "/hls/live/sample/1.ts"} {
		for _, prefix := range []string{"/before", "/hls"} {
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, prefix+route, nil))
			if response.Code != http.StatusOK || response.Body.String() != "proxied:"+route {
				test.Fatalf("local router intercepted proxy: %s status=%d body=%q", prefix+route, response.Code, response.Body.String())
			}
		}
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/hls/live/absent.m3u8", nil))
	if response.Code != http.StatusNotFound || strings.HasPrefix(response.Body.String(), "proxied:") {
		test.Fatal("proxy intercepted a local HLS request")
	}
}
