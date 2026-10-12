package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nextpkg/streamd/config"
)

type hlsObservation struct {
	lastFetch, segmentRequests, playlistRequests, window, deadline atomic.Int64
	server                                                         *httptest.Server
}

func observeHLS(origin string, minimum time.Duration) (*hlsObservation, error) {
	upstreamURL, err := url.Parse(origin)
	if err != nil {
		return nil, err
	}
	observation := &hlsObservation{}
	extend := func(counter *atomic.Int64, value int64) {
		for previous := counter.Load(); value > previous; previous = counter.Load() {
			if counter.CompareAndSwap(previous, value) {
				return
			}
		}
	}
	httpProxy := httputil.NewSingleHostReverseProxy(upstreamURL)
	httpProxy.ModifyResponse = func(response *http.Response) error {
		if response.StatusCode == http.StatusOK && response.Request.Method == http.MethodGet {
			media := false
			if strings.HasSuffix(response.Request.URL.Path, ".ts") {
				observation.segmentRequests.Add(1)
				media = true
			} else if strings.HasSuffix(response.Request.URL.Path, ".m3u8") {
				body, err := io.ReadAll(response.Body)
				closeError := response.Body.Close()
				if err != nil || closeError != nil {
					return errors.Join(err, closeError)
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				window, err := hlsPlaylistWindow(string(body))
				if err != nil {
					return err
				}
				observation.window.Store(int64(window))
				observation.playlistRequests.Add(1)
				media = true
			}
			if media {
				now := time.Now()
				extend(&observation.lastFetch, now.UnixNano())
				extend(&observation.deadline, now.Add(max(minimum, time.Duration(observation.window.Load()))).UnixNano())
			}
		}
		return nil
	}
	observation.server = httptest.NewServer(httpProxy)
	return observation, nil
}

func (harness *harness) hlsOrigin() (result error) {
	source, err := harness.originSource("source-hls")
	if err != nil {
		return err
	}
	defer source.cleanup()
	proxy, err := newOriginProxy(harness.ctx, fmt.Sprintf("127.0.0.1:%d", source.rtmpPort), false)
	if err != nil {
		return err
	}
	defer proxy.close()
	defer harness.cleanup()
	harness.settings.RTMP.EdgeAlive, harness.settings.RTMP.EdgeCheck = 3, 1
	harness.settings.Finder = &config.Finder{Expire: 120, Interval: 1, EnableRelay: true, Separator: "/"}
	harness.settings.Application = []appConfig{{Enable: true, AppName: "origin", StaticPull: []string{"rtmp://" + proxy.listener.Addr().String() + "/origin"},
		HLS:      hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 2},
		Recorder: recorderConfig{Enable: true, SaveAs: "ts", SaveMode: "immediate", Segment: 4, Driver: "fs", SaveTo: filepath.Join(harness.evidence, "recordings")}}}
	if err := harness.start(); err != nil {
		return err
	}
	status, _, err := harness.fetch("/hls/origin/sample.m3u8")
	if err != nil || status != http.StatusNotFound || proxy.accepted.Load() != 0 {
		return fmt.Errorf("HLS request changed original cold-trigger behavior: HTTP %d attempts=%d err=%v", status, proxy.accepted.Load(), err)
	}
	harness.results["hls_does_not_trigger_cold_origin"] = true
	publisher, err := source.publish("publish-hls-source", "origin", "bframes", 60)
	if err != nil {
		return err
	}
	if err := source.until("HLS source publishing", 3*time.Second, func() (bool, error) {
		report, err := source.relayStat("origin", "pub")
		return report.Count == 1, err
	}); err != nil {
		return err
	}
	bootstrap, err := harness.originViewer("hls-bootstrap-rtmp", fmt.Sprintf("rtmp://127.0.0.1:%d/origin/sample", harness.rtmpPort), 8)
	if err != nil {
		return err
	}
	if _, err := harness.waitPlaylist("origin"); err != nil {
		return err
	}
	observation, err := observeHLS(harness.origin, 3*time.Second)
	if err != nil {
		return err
	}
	defer observation.server.Close()
	player, err := harness.launch("play-hls-only", "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000", "-re",
		"-i", observation.server.URL+"/hls/origin/sample.m3u8", "-t", "28", "-progress", filepath.Join(harness.evidence, "progress-hls-only.txt"), "-f", "null", "-")
	if err != nil {
		return err
	}
	harness.results["hls_playback_paced"] = true
	players := map[string]*process{"hls-only": player}
	watch := harness.watchPlayback(players)
	joined := false
	defer func() {
		if !joined {
			result = errors.Join(result, watch.finish(harness))
		}
	}()
	if err := harness.finishPlayers(map[string]*process{"hls-bootstrap-rtmp": bootstrap}, 180); err != nil {
		return err
	}
	if err := harness.until("only HLS audience remains", 3*time.Second, func() (bool, error) {
		count, err := harness.originOnline()
		return count == 0, err
	}); err != nil {
		return err
	}
	if err := harness.sleep(7 * time.Second); err != nil {
		return err
	}
	edge, err := harness.relayStat("origin", "pub")
	if err != nil {
		return err
	}
	upstream, err := source.relayStat("origin", "pub")
	if err != nil {
		return err
	}
	harness.results["hls_only_after_idle_window"] = map[string]any{"wait_seconds": 7, "edge_alive_seconds": 3, "edge_publishers": edge.Count,
		"source_publishers": upstream.Count, "upstream_tcp_connections": proxy.active.Load(), "player_alive": player.alive(), "segments": observation.segmentRequests.Load()}
	if edge.Count != 1 || upstream.Count != 1 || proxy.active.Load() != 1 || !player.alive() || observation.segmentRequests.Load() < 3 {
		return errors.New("live HLS-only consumer lost its origin after the RTMP viewer left")
	}
	harness.settings.Application[0].HLS.Fragment = 4
	if err := harness.atomicSettings(); err != nil {
		return err
	}
	if err := harness.until("HLS fragment reload", 5*time.Second, func() (bool, error) {
		report, err := harness.configuration()
		return report.Revision == 2 && componentsApplied(report, 2, map[componentKey]string{{"hls", "origin/sample"}: "active", {"recording", "origin/sample"}: "active"}), err
	}); err != nil {
		return err
	}
	if err := harness.finishPlayers(players, 630); err != nil {
		return err
	}
	watchError := watch.finish(harness)
	joined = true
	if watchError != nil {
		return watchError
	}
	harness.results["hls_only_continuity"] = harness.results["playback_continuity"]
	lastRequest := time.Unix(0, observation.lastFetch.Load())
	expectedDeadline := time.Unix(0, observation.deadline.Load())
	if observation.lastFetch.Load() == 0 || proxy.accepted.Load() != 1 || proxy.active.Load() != 1 {
		return errors.New("HLS requests were missing, origin redialed, or origin ended before playback")
	}
	if err := harness.until("HLS audience idle cleanup", max(time.Second, time.Until(expectedDeadline)+3*time.Second), func() (bool, error) {
		report, err := harness.relayStat("origin", "pub")
		return report.Count == 0 && proxy.active.Load() == 0, err
	}); err != nil {
		return err
	}
	elapsed := time.Since(lastRequest).Seconds()
	deadlineDelay := time.Since(expectedDeadline).Seconds()
	upstream, err = source.relayStat("origin", "pub")
	if err != nil || upstream.Count != 1 || elapsed < 3 || deadlineDelay < -0.5 || deadlineDelay > 2.5 {
		return fmt.Errorf("HLS idle cleanup violated bounds or closed the publisher: elapsed=%.3f source=%+v err=%v", elapsed, upstream, err)
	}
	harness.results["hls_idle_cleanup"] = map[string]any{"seconds_after_last_fetch": elapsed, "playlist_requests": observation.playlistRequests.Load(), "segment_requests": observation.segmentRequests.Load(),
		"source_publisher_alive": true, "origin_connections": proxy.accepted.Load(), "remaining_origin_connections": proxy.active.Load(),
		"last_issued_window_seconds": time.Duration(observation.window.Load()).Seconds(), "expected_grace_after_last_fetch_seconds": expectedDeadline.Sub(lastRequest).Seconds(), "seconds_after_expected_deadline": deadlineDelay}
	if err := source.finish("publish-hls-source", publisher, 75*time.Second); err != nil {
		return err
	}
	for _, host := range []*harnessTypeAlias{harness, source} {
		if err := host.stopServer(); err != nil {
			return err
		}
		if err := host.checkLogs("play-*.log", "publish-*.log"); err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(host.evidence, "server.log"))
		if err != nil {
			return err
		}
		counts, err := validateHLSOriginLog(string(body), host == harness)
		if err != nil {
			return err
		}
		host.results["hls_origin_server_counts"] = counts
	}
	if err := harness.recordings("recordings", 6, true); err != nil {
		return err
	}
	harness.results["hls_origin_verified"] = true
	return nil
}

func hlsPlaylistWindow(body string) (time.Duration, error) {
	var target, segments int64
	for _, line := range strings.Split(body, "\n") {
		if value, found := strings.CutPrefix(strings.TrimSpace(line), "#EXT-X-TARGETDURATION:"); found {
			if target != 0 {
				return 0, errors.New("duplicate HLS target duration")
			}
			var err error
			target, err = strconv.ParseInt(value, 10, 64)
			if err != nil || target <= 0 {
				return 0, fmt.Errorf("invalid HLS target duration %q", value)
			}
		}
		if strings.HasPrefix(line, "#EXTINF:") {
			segments++
		}
	}
	if target <= 0 || segments == 0 || target > int64(time.Duration(1<<63-1)/time.Second)/segments {
		return 0, errors.New("missing or overflowing HLS window")
	}
	return time.Duration(target*segments) * time.Second, nil
}

func validateHLSOriginLog(body string, edge bool) (map[string]int, error) {
	counts := map[string]int{}
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event struct {
			Level   string `json:"level"`
			Message string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return counts, err
		}
		if event.Level == "ERROR" {
			return counts, fmt.Errorf("unexpected HLS origin error: %s", event.Message)
		}
		for name, prefix := range map[string]string{"publish": "[origin/sample]New PUBLISH from ", "play": "[origin/sample]New PLAY from ", "origin": "[origin/sample]Origin attached from ", "idle": "[origin/sample]Data source idling over "} {
			if strings.HasPrefix(event.Message, prefix) {
				counts[name]++
			}
		}
	}
	expected := map[string]int{"publish": 1, "play": 1, "origin": 0, "idle": 0}
	if edge {
		expected = map[string]int{"publish": 0, "play": 1, "origin": 1, "idle": 1}
	}
	for name, count := range expected {
		if counts[name] != count {
			return counts, fmt.Errorf("HLS origin %s=%d expected=%d", name, counts[name], count)
		}
	}
	return counts, nil
}
