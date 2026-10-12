package main

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nextpkg/streamd/config"
)

func TestDiscoveryEvidenceRequiresBothProtocolAuthorities(test *testing.T) {
	valid := `{"version":1,"rtmp":"source:1935","http":"source:8080"}`
	if !discoveryRecordMatches(valid, "source:1935", "source:8080") {
		test.Fatal("correct protocol advertisement rejected")
	}
	for _, value := range []string{"source:1935", `{"version":1,"rtmp":"source:8080","http":"source:1935"}`, `{"version":2,"rtmp":"source:1935","http":"source:8080"}`, `{"version":1,"rtmp":"source:1935"}`, `{"version":1,"rtmp":"source:1935","http":"source:8080","extra":true}`} {
		if discoveryRecordMatches(value, "source:1935", "source:8080") {
			test.Fatalf("incomplete or wrong protocol evidence accepted: %s", value)
		}
	}
}

func TestProxyDeadlineEvidenceRequiresOwnedFailures(test *testing.T) {
	origin := "http://127.0.0.1:32100"
	owned := map[string]bool{"127.0.0.1:32101": true}
	timeout := `{"level":"WARN","msg":"proxy response failed: write tcp 127.0.0.1:32100->127.0.0.1:32101: i/o timeout"}`
	cancelled := `{"level":"WARN","msg":"proxy transport: httputil: ReverseProxy read error during body copy: context canceled"}`
	counts, err := proxyDeadlineEvents(timeout+"\n"+cancelled, origin, owned)
	if err != nil || counts["127.0.0.1:32101"] != 1 || counts["cancelled_body_copy"] != 1 {
		test.Fatalf("owned failures rejected: %v %v", counts, err)
	}
	for _, body := range []string{
		strings.Replace(timeout, "32101", "32102", 1),
		strings.Replace(timeout, "32100", "32103", 1),
		strings.Replace(timeout, "i/o timeout", "broken pipe", 1),
		timeout + "\n" + timeout,
		cancelled + "\n" + cancelled,
		`{"level":"ERROR","msg":"proxy callback failed"}`,
		`{"level":"WARN","msg":"unrelated media failure"}`,
		"WARNING: DATA RACE",
		"panic: unexpected",
	} {
		if _, err := proxyDeadlineEvents(body, origin, owned); err == nil {
			test.Errorf("unexpected event accepted: %s", body)
		}
	}
}

func TestProxySignedURLKeepsRouteAndStreamIdentity(test *testing.T) {
	for _, route := range []string{"hfs/live/sample.flv", "hls/live/sample.m3u8", "hls/live/sample/1.ts"} {
		location, err := url.Parse(proxySignedURL("http://127.0.0.1:1234", "live", route, "test-key"))
		if err != nil {
			test.Fatal(err)
		}
		if location.Host != "127.0.0.1:1234" || location.Path != "/via/"+route {
			test.Fatalf("proxy URL changed route: %s", location)
		}
		query := location.Query()
		signature := config.NewSignatureSet("test-key", 60)
		if signature.Check("live/sample", query.Get("token"), query.Get("e")) != http.StatusOK {
			test.Fatal("valid stream rejected")
		}
		for _, key := range []string{"other/sample", "live/other"} {
			if signature.Check(key, query.Get("token"), query.Get("e")) != http.StatusForbidden {
				test.Fatalf("signature not bound to stream: %s", key)
			}
		}
	}
}

func TestProxyResponseMetricsRejectMissingOrAmbiguousCounts(test *testing.T) {
	valid := "streamd_proxy_requests_active 2\nstreamd_proxy_responses_aborted_total 4\n"
	active, aborted, err := proxyResponseCounts(valid)
	if err != nil || active != 2 || aborted != 4 {
		test.Fatalf("valid metrics rejected: %d %d %v", active, aborted, err)
	}
	for _, body := range []string{"", "streamd_proxy_requests_active 2\n", valid + "streamd_proxy_requests_active 2\n", strings.Replace(valid, "active 2", "active -1", 1), strings.Replace(valid, "total 4", "total 4.5", 1), strings.Replace(valid, "active 2", "active 2 extra", 1)} {
		if _, _, err := proxyResponseCounts(body); err == nil {
			test.Errorf("ambiguous evidence accepted: %q", body)
		}
	}
}

func TestProxySourceErrorsRequireOwnedPeerAndInjectionWindow(test *testing.T) {
	started := time.Date(2026, 10, 5, 1, 0, 0, 0, time.UTC)
	retired := started.Add(20 * time.Second)
	valid := `{"time":"2026-10-05T01:00:10Z","level":"ERROR","msg":"write tcp 127.0.0.1:32100->127.0.0.1:32101: i/o timeout"}`
	peers := map[string]bool{"127.0.0.1:32101": true, "127.0.0.1:32102": true, "127.0.0.1:32103": true}
	if counts, err := proxySourceDeadlineEvents(valid, "http://127.0.0.1:32100", peers, started, retired); err != nil || counts["127.0.0.1:32101"] != 1 {
		test.Fatalf("owned source deadline rejected: %v %v", counts, err)
	}
	for _, body := range []string{
		strings.Replace(valid, "32101", "32999", 1),
		strings.Replace(valid, "01:00:10", "00:59:59", 1),
		strings.Replace(valid, "01:00:10", "01:00:21", 1),
		strings.Replace(valid, "i/o timeout", "corrupt media", 1),
		valid + "\n" + valid,
		valid + "\n" + strings.Replace(valid, "32101", "32102", 1) + "\n" + strings.Replace(valid, "32101", "32103", 1),
		"WARNING: DATA RACE",
	} {
		if _, err := proxySourceDeadlineEvents(body, "http://127.0.0.1:32100", peers, started, retired); err == nil {
			test.Errorf("unexpected source failure accepted: %s", body)
		}
	}
}
