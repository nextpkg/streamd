package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nextpkg/streamd/config"
)

func proxySignedURL(origin, app, suffix, secret string) string {
	expiration := time.Now().Unix() + 60
	return fmt.Sprintf("%s/via/%s?e=%d&token=%s", origin, suffix, expiration, config.AntiLeechSign(app+"/sample", secret, expiration))
}

func discoveryRecordMatches(value, rtmp, http string) bool {
	var record map[string]any
	if json.Unmarshal([]byte(value), &record) != nil {
		return false
	}
	return len(record) == 3 && record["version"] == float64(1) && record["rtmp"] == rtmp && record["http"] == http
}

func stalledHTTPDialer() net.Dialer {
	return net.Dialer{Timeout: 3 * time.Second, Control: func(network, address string, raw syscall.RawConn) error {
		var socketError error
		if err := raw.Control(func(descriptor uintptr) {
			socketError = syscall.SetsockoptInt(int(descriptor), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 1024)
		}); err != nil {
			return err
		}
		return socketError
	}}
}

func proxyResponseCounts(body string) (int, int, error) {
	values := map[string]int{}
	for _, line := range strings.Split(body, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || (fields[0] != "streamd_proxy_requests_active" && fields[0] != "streamd_proxy_responses_aborted_total") {
			continue
		}
		if len(fields) != 2 {
			return 0, 0, errors.New("invalid proxy metric")
		}
		count, err := strconv.Atoi(fields[1])
		if err != nil || count < 0 {
			return 0, 0, errors.New("invalid proxy response count")
		}
		if _, exists := values[fields[0]]; exists {
			return 0, 0, errors.New("duplicate proxy metric")
		}
		values[fields[0]] = count
	}
	if len(values) != 2 {
		return 0, 0, errors.New("missing proxy lifecycle metrics")
	}
	return values["streamd_proxy_requests_active"], values["streamd_proxy_responses_aborted_total"], nil
}

func (harness *harness) proxyResponses() (int, int, error) {
	status, body, err := harness.fetch("/metrics")
	if err != nil {
		return 0, 0, err
	}
	if status != http.StatusOK {
		return 0, 0, fmt.Errorf("proxy metrics HTTP %d", status)
	}
	return proxyResponseCounts(string(body))
}

func (harness *harness) stallProxyFLV(location string) (*stalledSubscriber, error) {
	address, err := url.Parse(location)
	if err != nil {
		return nil, err
	}
	dialer := stalledHTTPDialer()
	transport, err := dialer.DialContext(harness.ctx, "tcp", address.Host)
	if err != nil {
		return nil, err
	}
	connection := transport.(*net.TCPConn)
	ready := false
	defer func() {
		if !ready {
			_ = connection.Close()
		}
	}()
	if err := connection.SetReadBuffer(1024); err != nil {
		return nil, err
	}
	if err := connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodGet, location, nil)
	if err != nil {
		return nil, err
	}
	if err := request.Write(connection); err != nil {
		return nil, err
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("slow proxy FLV HTTP %d", response.StatusCode)
	}
	header := make([]byte, 9)
	if _, err := io.ReadFull(response.Body, header); err != nil {
		return nil, err
	}
	if string(header[:3]) != "FLV" {
		return nil, errors.New("slow proxy FLV response lacks header")
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	ready = true
	return &stalledSubscriber{connection: connection, reader: response.Body, protocol: "proxy-http-flv", local: connection.LocalAddr().String()}, nil
}

func proxyDeadlineEvents(body, origin string, addresses map[string]bool) (map[string]int, error) {
	endpoint, err := url.Parse(origin)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		var event struct {
			Level   string `json:"level"`
			Message string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, err
		}
		if event.Level == "ERROR" {
			return nil, fmt.Errorf("unexpected proxy service error: %s", event.Message)
		}
		if event.Level != "WARN" {
			continue
		}
		if event.Message == "proxy transport: httputil: ReverseProxy read error during body copy: context canceled" {
			counts["cancelled_body_copy"]++
			if counts["cancelled_body_copy"] > len(addresses) {
				return nil, errors.New("too many cancelled proxy body copies")
			}
			continue
		}
		peer, matched := strings.CutPrefix(event.Message, "proxy response failed: write tcp "+endpoint.Host+"->")
		peer, timeout := strings.CutSuffix(peer, ": i/o timeout")
		if !matched || !timeout || !addresses[peer] {
			return nil, fmt.Errorf("unexpected proxy warning: %s", event.Message)
		}
		counts[peer]++
		if counts[peer] > 1 {
			return nil, fmt.Errorf("duplicate proxy deadline warning: %s", peer)
		}
	}
	return counts, nil
}

func proxySourceDeadlineEvents(body, source string, peers map[string]bool, started, retired time.Time) (map[string]int, error) {
	endpoint, err := url.Parse(source)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	total := 0
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		var event struct {
			Time    string `json:"time"`
			Level   string `json:"level"`
			Message string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, err
		}
		if event.Level != "ERROR" && event.Level != "WARN" {
			continue
		}
		peer, matched := strings.CutPrefix(event.Message, "write tcp "+endpoint.Host+"->")
		peer, timeout := strings.CutSuffix(peer, ": i/o timeout")
		observed, err := time.Parse(time.RFC3339Nano, event.Time)
		if err != nil || !matched || !timeout || !peers[peer] || observed.Before(started) || observed.After(retired) {
			return nil, fmt.Errorf("unexpected source backpressure event: %s", line)
		}
		counts[peer]++
		total++
		if total > 2 || counts[peer] > 1 {
			return nil, errors.New("source backpressure exceeded two owned slow TS responses")
		}
	}
	return counts, nil
}

func (harness *harness) proxyMedia() (failure error) {
	registry, err := harness.startRedisContainer("proxy")
	if err != nil {
		return err
	}
	defer func() { failure = errors.Join(failure, registry.close(harness)) }()
	defer harness.cleanup()
	fixture, err := harness.pressureFixtureWithBitrate("24M")
	if err != nil {
		return err
	}
	source, err := harness.originSource("source")
	if err != nil {
		return err
	}
	defer source.cleanup()
	source.settings.Application = nil
	source.recordingApplications("immediate", 4, 6)
	const previousSecret = "proxy-previous-test-key"
	const currentSecret = "proxy-current-test-key"
	for index := range source.settings.Application {
		source.settings.Application[index].HLS.SecureKey = previousSecret
		source.settings.Application[index].HLS.SecureTTL = 60
	}
	sourceAddress, err := url.Parse(source.origin)
	if err != nil {
		return err
	}
	sourceBridge, err := newOriginProxy(harness.ctx, sourceAddress.Host, false)
	if err != nil {
		return err
	}
	defer sourceBridge.close()
	nextBridge, err := newOriginProxy(harness.ctx, sourceAddress.Host, false)
	if err != nil {
		return err
	}
	defer nextBridge.close()
	source.settings.Finder = &config.Finder{Advice: source.settings.RTMP.Addr, HTTPAdvice: sourceBridge.listener.Addr().String(), Expire: 120, Interval: 1, EnableRelay: true, Separator: "/",
		Redis: &config.RedisConfig{Address: registry.address, Password: redisTestPassword, DB: 13}}
	if err := source.commit("configure-signed-source-and-protocol-advertisements", nil); err != nil {
		return err
	}
	callbackEntered, callbackCancelled := make(chan struct{}), make(chan struct{})
	callback := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		close(callbackEntered)
		<-request.Context().Done()
		close(callbackCancelled)
	}))
	defer callback.Close()
	harness.settings.Proxy = &config.Proxy{Prefix: "via", EnableSecure: true, SecureKey: previousSecret, SecureTTL: 60, OnNotFound: callback.URL}
	harness.settings.Finder = &config.Finder{Advice: fmt.Sprintf("127.0.0.1:%d", harness.rtmpPort), Expire: 120, Interval: 1, EnableRelay: true, Separator: "/",
		Redis: &config.RedisConfig{Address: registry.address, Password: redisTestPassword, DB: 13}}
	harness.settings.Application = []appConfig{{Enable: true, AppName: "flv"}, {Enable: true, AppName: "ts"}, {Enable: true, AppName: "guard",
		HLS: hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 6}, Recorder: recorderConfig{Enable: true, SaveAs: "ts", SaveMode: "immediate", Segment: 4, Driver: "fs", SaveTo: filepath.Join(harness.evidence, "recordings")}}}
	if err := harness.start(); err != nil {
		return err
	}
	harness.results["endpoint_contract"] = map[string]any{"initial_proxy_backend": sourceBridge.listener.Addr().String(), "replacement_proxy_backend": nextBridge.listener.Addr().String(), "source_http_address": sourceAddress.Host, "source_rtmp_address": source.settings.RTMP.Addr, "registry_values": "source-produced version-1 JSON with independent RTMP and HTTP authorities", "source_registry": "redis", "combined_http_rtmp_port_discovery_verified": false}
	minimumExpiry := time.Time{}
	captureDiscovery := func(label, expectedHTTP string) error {
		snapshot := map[string]any{}
		if err := harness.until("protocol-specific registry "+label, 5*time.Second, func() (bool, error) {
			for _, app := range []string{"flv", "ts"} {
				key := "/streamd/" + app + "/sample"
				value, err := registry.client.Get(harness.ctx, key).Result()
				if err != nil || !discoveryRecordMatches(value, source.settings.RTMP.Addr, expectedHTTP) {
					return false, nil
				}
				ttl, err := registry.client.PTTL(harness.ctx, key).Result()
				if err != nil || ttl <= 0 || ttl > 120*time.Second {
					return false, err
				}
				expiry := time.Now().Add(ttl)
				if minimumExpiry.IsZero() || expiry.Before(minimumExpiry) {
					minimumExpiry = expiry
				}
				snapshot[app] = map[string]any{"key": key, "value": value, "ttl_milliseconds": ttl.Milliseconds(), "expires_no_earlier_than": expiry}
			}
			return true, nil
		}); err != nil {
			return err
		}
		harness.results["discovery_records_"+label] = snapshot
		return nil
	}
	harness.results["publish_seconds"] = 65
	publishers := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		child, err := source.launch("publish-"+app, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-re", "-stream_loop", "-1", "-i", fixture, "-t", "65", "-c", "copy", "-f", "flv", fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", source.rtmpPort, app))
		if err != nil {
			return err
		}
		publishers[app] = child
	}
	guard, err := harness.launch("publish-guard", "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-re", "-stream_loop", "-1", "-i", fixture, "-t", "50", "-c", "copy", "-f", "flv", fmt.Sprintf("rtmp://127.0.0.1:%d/guard/sample", harness.rtmpPort))
	if err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		if _, err := source.waitPlaylist(app); err != nil {
			return err
		}
	}
	if _, err := harness.waitPlaylist("guard"); err != nil {
		return err
	}
	if err := captureDiscovery("initial", sourceBridge.listener.Addr().String()); err != nil {
		return err
	}
	players, err := harness.pacedOriginPlayers("guard", "guard-", 36)
	if err != nil {
		return err
	}
	oldPlaylists := map[string]string{}
	for _, app := range []string{"flv", "ts"} {
		name := "discovered-" + app + "-rtmp"
		players[name], err = harness.launch("play-"+name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000", "-re", "-i", fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app), "-t", "36", "-progress", filepath.Join(harness.evidence, "progress-"+name+".txt"), "-f", "null", "-")
		if err != nil {
			return err
		}
		for protocol, suffix := range map[string]string{"http-flv": "hfs/" + app + "/sample.flv", "hls": "hls/" + app + "/sample.m3u8"} {
			location := proxySignedURL(harness.origin, app, suffix, previousSecret)
			if protocol == "hls" {
				oldPlaylists[app] = location
			}
			name := "proxy-" + app + "-" + protocol
			players[name], err = harness.launch("play-"+name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000", "-re", "-i", location, "-t", "36", "-progress", filepath.Join(harness.evidence, "progress-"+name+".txt"), "-f", "null", "-")
			if err != nil {
				return err
			}
		}
	}
	watch := harness.watchPlayback(players)
	defer func() { failure = errors.Join(failure, watch.finish(harness)) }()
	if err := watch.waitReady(harness.ctx); err != nil {
		return fmt.Errorf("proxy playback warmup: %w", err)
	}
	baseline := map[string]int{}
	for _, app := range []string{"flv", "ts"} {
		stat, err := source.relayStat(app, "sub")
		if err != nil {
			return err
		}
		baseline[app] = stat.Count
	}
	before, err := harness.goroutines()
	if err != nil {
		return err
	}
	abortedBefore := 0
	activeBefore := 0
	if err := harness.until("live proxy viewers registered", 5*time.Second, func() (bool, error) {
		active, aborted, err := harness.proxyResponses()
		activeBefore, abortedBefore = active, aborted
		return active >= 2 && active <= 6, err
	}); err != nil {
		return err
	}
	var stalledFLV []*stalledSubscriber
	var stalledTS []*stalledSegment
	defer func() {
		for _, viewer := range stalledFLV {
			_ = viewer.connection.Close()
		}
		for _, viewer := range stalledTS {
			_ = viewer.connection.Close()
		}
	}()
	retained := map[string][]byte{}
	injectionStarted := time.Now()
	addresses := map[string]bool{}
	clients := []map[string]any{}
	if err := os.MkdirAll(filepath.Join(harness.evidence, "segments"), 0700); err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		viewer, err := harness.stallProxyFLV(proxySignedURL(harness.origin, app, "hfs/"+app+"/sample.flv", previousSecret))
		if err != nil {
			return err
		}
		stalledFLV = append(stalledFLV, viewer)
		addresses[viewer.local] = true
		clients = append(clients, map[string]any{"protocol": "http-flv", "local": viewer.local, "remote": viewer.connection.RemoteAddr().String()})
		harness.results["stalled_proxy_clients"] = clients
		location, body, err := source.largeSegment(app)
		if err != nil {
			return err
		}
		location = harness.origin + "/via" + strings.TrimPrefix(location, source.origin)
		retained[location] = body
		if err := os.WriteFile(filepath.Join(harness.evidence, "segments", app+".ts"), body, 0600); err != nil {
			return err
		}
		segment, err := harness.stallSegmentWithDialer(location, stalledHTTPDialer())
		if err != nil {
			return err
		}
		stalledTS = append(stalledTS, segment)
		addresses[segment.local] = true
		clients = append(clients, map[string]any{"protocol": "ts", "local": segment.local, "remote": segment.connection.RemoteAddr().String(), "location": location, "content_length": segment.response.ContentLength})
		harness.results["stalled_proxy_clients"] = clients
	}
	peak, err := harness.goroutines()
	if err != nil {
		return err
	}
	if peak < before+6 {
		return fmt.Errorf("proxy stalled response owners not observed: baseline=%d peak=%d", before, peak)
	}
	harness.results["stalled_proxy_goroutines"] = map[string]int{"before": before, "peak": peak}
	peakActive, _, err := harness.proxyResponses()
	if err != nil {
		return err
	}
	if peakActive < 6 {
		return fmt.Errorf("expected six active proxy requests, got %d", peakActive)
	}
	harness.settings.Proxy.SecureKey = currentSecret
	harness.settings.Proxy.Prefix = "via/handoff"
	if err := harness.commit("rotate-proxy-signature-and-prefix-with-live-requests", nil); err != nil {
		return err
	}
	aliases := []string{"via", "via/handoff"}
	transitions := []map[string]any{}
	checkAliases := func() error {
		segments := []map[string]any{}
		playlists := []string{}
		for _, prefix := range aliases {
			for location, expected := range retained {
				address := strings.Replace(location, harness.origin+"/via/", harness.origin+"/"+prefix+"/", 1)
				status, body, err := harness.fetch(address)
				if err != nil || status != http.StatusOK || !bytes.Equal(body, expected) {
					return fmt.Errorf("prefix %s lost issued segment: status=%d error=%v", prefix, status, err)
				}
				segments = append(segments, map[string]any{"location": address, "sha256": fmt.Sprintf("%x", sha256.Sum256(body)), "bytes": len(body), "identical": true})
			}
			for app := range oldPlaylists {
				address := proxySignedURL(harness.origin, app, "hls/"+app+"/sample.m3u8", currentSecret)
				address = strings.Replace(address, harness.origin+"/via/", harness.origin+"/"+prefix+"/", 1)
				status, body, err := harness.fetch(address)
				if err != nil || status != http.StatusOK || !strings.Contains(string(body), "#EXTINF") {
					return fmt.Errorf("prefix %s lost playlist: status=%d error=%v", prefix, status, err)
				}
				playlists = append(playlists, address)
			}
		}
		transitions = append(transitions, map[string]any{"current_prefix": harness.settings.Proxy.Prefix, "aliases": append([]string(nil), aliases...), "segments": segments, "playlists": playlists})
		harness.results["prefix_transition_checks"] = transitions
		return nil
	}
	if err := checkAliases(); err != nil {
		return err
	}
	expected := map[componentKey]string{}
	for index := range source.settings.Application {
		app := &source.settings.Application[index]
		app.HLS.SecureKey, app.HLS.PlaylistLength = currentSecret, 3
		expected[componentKey{"hls", app.AppName + "/sample"}] = "active"
		expected[componentKey{"recording", app.AppName + "/sample"}] = "active"
	}
	if err := source.commit("rotate-source-signature-and-resize-playlists", expected); err != nil {
		return err
	}
	harness.settings.Proxy.Prefix = "hls"
	if err := harness.commit("move-proxy-prefix-again-with-live-requests", nil); err != nil {
		return err
	}
	aliases = append(aliases, "hls")
	if err := checkAliases(); err != nil {
		return err
	}
	retiredAt := time.Now()
	retired := 0
	abortedAfter := 0
	if err := harness.until("proxy stalled responses retire before read resumes", 18*time.Second, func() (bool, error) {
		active, aborted, err := harness.proxyResponses()
		abortedAfter = aborted
		observations, _ := harness.results["proxy_retirement_samples"].([]map[string]any)
		observation := map[string]any{"seconds": time.Since(retiredAt).Seconds(), "active": active, "aborted": aborted}
		harness.results["proxy_retirement_samples"] = append(observations, observation)
		if err != nil || active < 2 || active > 6 || aborted != abortedBefore+4 {
			return false, err
		}
		for _, app := range []string{"flv", "ts"} {
			stat, err := source.relayStat(app, "sub")
			observation[app+"_subscribers"] = stat.Count
			if err != nil || stat.Count != baseline[app] {
				return false, err
			}
		}
		retired, err = harness.goroutines()
		observation["goroutines"] = retired
		return retired <= before+4, err
	}); err != nil {
		return err
	}
	harness.results["stalled_retirement_before_read_resumed"] = map[string]any{"clients": 4, "source_subscribers_before": baseline, "source_subscribers_restored": true, "goroutines_before": before, "goroutines_peak": peak, "goroutines_retired": retired, "maximum_residual_goroutines": 4, "seconds_after_reload": time.Since(retiredAt).Seconds()}
	activeRetired, _, err := harness.proxyResponses()
	if err != nil {
		return err
	}
	harness.results["proxy_response_ownership"] = map[string]any{"active_before": activeBefore, "active_peak": peakActive, "active_retired": activeRetired, "active_healthy_range": []int{2, 6}, "aborted_before": abortedBefore, "aborted_after": abortedAfter, "clients_still_not_reading": 4}
	injectionRetired := time.Now()
	closures := []map[string]any{}
	for _, viewer := range stalledFLV {
		if err := viewer.connection.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			return err
		}
		count, err := io.Copy(io.Discard, io.LimitReader(viewer.reader, 16<<20))
		if count >= 16<<20 || (err != nil && !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, io.ErrUnexpectedEOF)) {
			return fmt.Errorf("proxy FLV did not close: bytes=%d error=%v", count, err)
		}
		closures = append(closures, map[string]any{"protocol": "http-flv", "local": viewer.local, "bytes_after_retirement": count, "peer_closed": true, "terminal_error": fmt.Sprint(err)})
	}
	for _, viewer := range stalledTS {
		result, err := viewer.closed()
		if err != nil {
			return err
		}
		closures = append(closures, result)
	}
	harness.results["expired_proxy_responses"] = closures
	source.settings.Finder.HTTPAdvice = nextBridge.listener.Addr().String()
	if err := source.commit("rotate-http-endpoint-after-response-retirement", expected); err != nil {
		return err
	}
	if err := captureDiscovery("after_reload", nextBridge.listener.Addr().String()); err != nil {
		return err
	}
	if err := harness.until("proxy HTTP discovery follows new authority", 5*time.Second, func() (bool, error) {
		status, _, err := harness.fetch(proxySignedURL(harness.origin, "flv", "hls/flv/sample.m3u8", currentSecret))
		return nextBridge.accepted.Load() > 0 && status == http.StatusOK, err
	}); err != nil {
		return err
	}
	if err := checkAliases(); err != nil {
		return err
	}
	preserved := []map[string]any{}
	for location, body := range retained {
		status, actual, err := harness.fetch(location)
		if err != nil || status != http.StatusOK || !bytes.Equal(actual, body) {
			return fmt.Errorf("old signed proxy segment changed after rotation: status=%d err=%v", status, err)
		}
		preserved = append(preserved, map[string]any{"location": location, "sha256": fmt.Sprintf("%x", sha256.Sum256(body)), "bytes": len(body), "identical_after_reload": true})
	}
	for app, location := range oldPlaylists {
		status, body, err := harness.fetch(location)
		if err != nil || status != http.StatusOK || !strings.Contains(string(body), "#EXTINF") {
			return fmt.Errorf("old signed playlist failed: %s status=%d error=%v", app, status, err)
		}
	}
	harness.results["retained_signed_segments"] = preserved
	harness.results["issued_playlist_links_survive_rotation"] = true
	if err := harness.finishPlayers(players, 875); err != nil {
		return err
	}
	if err := harness.finish("publish-guard", guard, 60*time.Second); err != nil {
		return err
	}
	tailPlayers := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		location := proxySignedURL(harness.origin, app, "hfs/"+app+"/sample.flv", currentSecret)
		tailPlayers[app], err = harness.launch("tail-"+app, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000", "-i", location, "-t", "120", "-progress", filepath.Join(harness.evidence, "tail-progress-"+app+".txt"), "-f", "null", "-")
		if err != nil {
			return err
		}
	}
	activeFrames := map[string]int{}
	if err := harness.until("tail proxy viewers decode before shutdown", 10*time.Second, func() (bool, error) {
		for app, child := range tailPlayers {
			if !child.alive() {
				return false, fmt.Errorf("tail %s exited before shutdown", app)
			}
			body, err := os.ReadFile(filepath.Join(harness.evidence, "tail-progress-"+app+".txt"))
			if errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			frames, err := completeProgressFrames(string(body))
			if err != nil {
				return false, err
			}
			activeFrames[app] = frames
			if frames < 25 {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		location, _, err := source.largeSegment(app)
		if err != nil {
			return err
		}
		viewer, err := harness.stallSegmentWithDialer(harness.origin+"/via"+strings.TrimPrefix(location, source.origin), stalledHTTPDialer())
		if err != nil {
			return err
		}
		stalledTS = append(stalledTS, viewer)
		addresses[viewer.local] = true
	}
	missingExpiration := time.Now().Unix() + 60
	missingURL := fmt.Sprintf("%s/via/hfs/flv/missing.flv?e=%d&token=%s", harness.origin, missingExpiration, config.AntiLeechSign("flv/missing", currentSecret, missingExpiration))
	callbackClient := &http.Client{Timeout: 5 * time.Second}
	callbackFinished := make(chan struct{})
	go func() {
		defer close(callbackFinished)
		response, err := callbackClient.Get(missingURL)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}()
	select {
	case <-callbackEntered:
	case <-time.After(2 * time.Second):
		return errors.New("shutdown callback did not start")
	case <-harness.ctx.Done():
		return harness.ctx.Err()
	}
	for app, publisher := range publishers {
		if !publisher.alive() {
			return fmt.Errorf("source publisher %s stopped before edge shutdown", app)
		}
	}
	shutdown := time.Now()
	if err := harness.stopServer(); err != nil {
		return err
	}
	elapsed := time.Since(shutdown).Seconds()
	if elapsed > 3 {
		return fmt.Errorf("proxy shutdown exceeded bound: %.3fs", elapsed)
	}
	select {
	case <-callbackCancelled:
	case <-time.After(time.Second):
		return errors.New("proxy shutdown retained callback")
	}
	select {
	case <-callbackFinished:
	case <-time.After(time.Second):
		return errors.New("proxy shutdown retained downstream callback request")
	}
	shutdownClosures := []map[string]any{}
	for _, viewer := range stalledTS[2:] {
		result, err := viewer.closed()
		if err != nil {
			return err
		}
		shutdownClosures = append(shutdownClosures, result)
	}
	disconnections := map[string]any{}
	for app, child := range tailPlayers {
		code, err := child.wait(harness.ctx, 8*time.Second)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(harness.evidence, "tail-"+app+".log"))
		if err != nil {
			return err
		}
		if err := validateInterruptedOrigin(code, string(body)); err != nil {
			return err
		}
		disconnections[app] = map[string]any{"exit_code": code, "stderr": string(body), "frames_before_sigterm": activeFrames[app], "active_before_sigterm": true}
	}
	harness.results["graceful_shutdown_with_proxy_requests"] = map[string]any{"seconds": elapsed, "exit_code": 0, "active_media": disconnections, "stalled_segments": shutdownClosures, "pending_callback_cancelled": true, "source_publishers_active": 2}
	if err := source.finishPublishers(publishers, "publish-", 30*time.Second); err != nil {
		return err
	}
	if err := source.until("typed registrations removed before TTL expires", 3*time.Second, func() (bool, error) {
		if !time.Now().Before(minimumExpiry) {
			return false, errors.New("registry cleanup cannot be distinguished from TTL expiry")
		}
		count, err := registry.client.Exists(harness.ctx, "/streamd/flv/sample", "/streamd/ts/sample").Result()
		return count == 0, err
	}); err != nil {
		return err
	}
	harness.results["typed_registrations_removed_before_ttl"] = true
	if err := source.stopServer(); err != nil {
		return err
	}
	if err := source.recordings("recordings", 20, true); err != nil {
		return err
	}
	for _, directory := range []string{"recordings", "segments"} {
		if err := harness.recordings(directory, 1, true); err != nil {
			return err
		}
	}
	if err := source.checkLogs("publish-*.log"); err != nil {
		return err
	}
	if err := harness.checkLogs("publish-*.log", "play-*.log", "generate-*.log"); err != nil {
		return err
	}
	sourcePeers := map[string]bool{}
	sourceBridge.upstreamPeers.Range(func(peer, _ any) bool { sourcePeers[peer.(string)] = true; return true })
	nextBridge.upstreamPeers.Range(func(peer, _ any) bool { sourcePeers[peer.(string)] = true; return true })
	harness.results["source_connection_observer"] = map[string]any{"upstream_peers": sourcePeers, "fault_started": injectionStarted, "fault_retired": injectionRetired, "maximum_expected_source_timeouts": 2}
	for _, service := range []*harnessTypeAlias{harness, source} {
		body, err := os.ReadFile(filepath.Join(service.evidence, "server.log"))
		if err != nil {
			return err
		}
		var counts map[string]int
		if service == source {
			counts, err = proxySourceDeadlineEvents(string(body), source.origin, sourcePeers, injectionStarted, injectionRetired)
		} else {
			counts, err = proxyDeadlineEvents(string(body), harness.origin, addresses)
		}
		if err != nil {
			return err
		}
		service.results["expected_proxy_deadline_events"] = counts
	}
	harness.results["source_publishers_complete_without_errors"] = true
	harness.results["endpoint_contract"].(map[string]any)["combined_http_rtmp_port_discovery_verified"] = true
	harness.results["http_endpoint_reload"] = map[string]any{"first_accepted": sourceBridge.accepted.Load(), "replacement_accepted": nextBridge.accepted.Load(), "healthy_decoders_continue": true, "same_source_media": true}
	harness.results["normal_player_count"] = len(players)
	harness.results["planned_disconnect_player_count"] = len(tailPlayers)
	harness.results["proxy_prefix_change_verified"] = true
	harness.results["proxy_prefix_continuity_scope"] = "Two live prefix changes including a nested prefix and hls, which shares the local HLS route name; original signed playlists and TS links remain usable, each committed alias resolves byte-identical retained TS and valid playlists, nine existing decoders continue across an HTTP discovery endpoint change, and later FLV requests use the original alias under current security. Aliases last for the current process only; no persisted historical credentials or discovery clients."
	return nil
}
