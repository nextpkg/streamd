package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/config"
)

func (harness *harness) relayRequest(method, action, pull, push string, expected int) error {
	form := url.Values{"pull": {pull}, "push": {push}}
	request, err := http.NewRequestWithContext(harness.ctx, method, harness.origin+"/admin/relay/"+action, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := harness.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		return err
	}
	var envelope struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	code := expected
	if expected == http.StatusOK {
		code = 0
	}
	if response.StatusCode != expected || envelope.Code != code {
		return fmt.Errorf("%s relay/%s: HTTP %d, expected %d: %s", method, action, response.StatusCode, expected, body)
	}
	return nil
}

func (harness *harness) relayConcurrent(action, pull, push string) error {
	var workers sync.WaitGroup
	failures := make(chan error, 16)
	for request := 0; request < 16; request++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			failures <- harness.relayRequest(http.MethodPost, action, pull, push, http.StatusOK)
		}()
	}
	workers.Wait()
	close(failures)
	var result error
	for failure := range failures {
		result = errors.Join(result, failure)
	}
	return result
}

func (harness *harness) relayStat(app, kind string) (subscriberReport, error) {
	var envelope struct {
		Code int              `json:"code"`
		Data subscriberReport `json:"data"`
	}
	status, body, err := harness.fetch("/admin/stat/" + kind + "/info?key=" + url.QueryEscape(app+"/sample"))
	if err != nil {
		return envelope.Data, err
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return envelope.Data, err
	}
	if status != http.StatusOK || envelope.Code != 0 {
		return envelope.Data, fmt.Errorf("%s %s stat HTTP %d: %s", app, kind, status, body)
	}
	return envelope.Data, nil
}

func (harness *harness) waitRelayPlaylistStopped(app string) error {
	return harness.until("stopped playlist for "+app, 3*time.Second, func() (bool, error) {
		status, body, err := harness.fetch("/hls/" + app + "/sample.m3u8")
		if err != nil {
			return false, err
		}
		return status == http.StatusNotFound || (status == http.StatusOK && strings.Contains(string(body), "#EXT-X-ENDLIST")), nil
	})
}

func (harness *harness) relayMedia() (result error) {
	sourceRTMP, err := freePort()
	if err != nil {
		return err
	}
	sourceHTTP, err := freePort()
	if err != nil {
		return err
	}
	source := &harnessTypeAlias{ctx: harness.ctx, root: harness.root, evidence: filepath.Join(harness.evidence, "source"), rtmpPort: sourceRTMP,
		origin: fmt.Sprintf("http://127.0.0.1:%d", sourceHTTP), httpClient: &http.Client{Timeout: 2 * time.Second}, results: map[string]any{}}
	if err := os.MkdirAll(source.evidence, 0700); err != nil {
		return err
	}
	defer source.cleanup()
	harness.results["source_service"] = source.results
	source.settings.HTTP.Addr = fmt.Sprintf("127.0.0.1:%d", sourceHTTP)
	source.settings.RTMP.Addr = fmt.Sprintf("127.0.0.1:%d", sourceRTMP)
	source.settings.Application = []appConfig{{Enable: true, AppName: "source", HLS: hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 3},
		StaticPush: []string{fmt.Sprintf("rtmp://127.0.0.1:%d/static", harness.rtmpPort)}}}
	sourceConfig := filepath.Join(source.evidence, "config.json")
	if err := writeJSON(sourceConfig, source.settings); err != nil {
		return err
	}
	source.server, err = source.launch("server", filepath.Join(harness.evidence, "streamd"), "-config", sourceConfig)
	if err != nil {
		return err
	}
	if err := source.until("source readiness", 10*time.Second, func() (bool, error) {
		status, _, err := source.fetch("/readyz")
		return status == http.StatusOK, err
	}); err != nil {
		return err
	}
	source.started = time.Now()
	publisher, err := source.publish("publish-source", "source", "bframes", 60)
	if err != nil {
		return err
	}
	if err := source.until("initial static destination refused", 5*time.Second, func() (bool, error) {
		body, err := os.ReadFile(filepath.Join(source.evidence, "server.log"))
		return strings.Contains(string(body), "dial relay pusher") && strings.Contains(string(body), "connection refused"), err
	}); err != nil {
		return err
	}
	harness.results["initial_static_target_unavailable"] = true
	harness.settings.Console = &config.Console{EnableRelay: true}
	for _, app := range []string{"static", "pull", "dynamic"} {
		settings := appConfig{Enable: true, AppName: app, HLS: hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 3},
			Recorder: recorderConfig{Enable: true, SaveAs: "flv", SaveMode: "immediate", Segment: 4, Driver: "fs", SaveTo: filepath.Join(harness.evidence, "recordings")}}
		if app == "pull" {
			settings.StaticPull = []string{fmt.Sprintf("rtmp://127.0.0.1:%d/source", sourceRTMP)}
			settings.Recorder.SaveAs = "ts"
		}
		harness.settings.Application = append(harness.settings.Application, settings)
	}
	if err := harness.start(); err != nil {
		return err
	}
	pullURL := fmt.Sprintf("rtmp://127.0.0.1:%d/source/sample", sourceRTMP)
	pushURL := fmt.Sprintf("rtmp://127.0.0.1:%d/dynamic/sample", harness.rtmpPort)
	for _, request := range []struct {
		method, action, pull, push string
		status                     int
	}{
		{http.MethodGet, "start", pullURL, pushURL, http.StatusMethodNotAllowed},
		{http.MethodPost, "unknown", pullURL, pushURL, http.StatusBadRequest},
		{http.MethodPost, "start", "", pushURL, http.StatusBadRequest},
		{http.MethodPost, "start", pullURL, "rtmp://127.0.0.1/app/", http.StatusBadRequest},
		{http.MethodPost, "start", pullURL, pullURL, http.StatusBadRequest},
		{http.MethodPost, "stop", pullURL, pushURL, http.StatusOK},
	} {
		if err := harness.relayRequest(request.method, request.action, request.pull, request.push, request.status); err != nil {
			return err
		}
	}
	harness.results["admin_parameter_and_method_cases"] = 6
	if err := harness.relayConcurrent("start", pullURL, pushURL); err != nil {
		return err
	}
	if _, err := harness.waitPlaylist("dynamic"); err != nil {
		return err
	}
	bootstrap, err := harness.launch("play-pull-bootstrap", "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000",
		"-i", fmt.Sprintf("rtmp://127.0.0.1:%d/pull/sample", harness.rtmpPort), "-t", "36", "-progress", filepath.Join(harness.evidence, "progress-pull-bootstrap.txt"), "-f", "null", "-")
	if err != nil {
		return err
	}
	if _, err := harness.waitPlaylist("pull"); err != nil {
		return err
	}
	sourcePlayers, err := source.players("source", "source-", 36)
	if err != nil {
		return err
	}
	pullPlayers, err := harness.players("pull", "pull-", 36)
	if err != nil {
		return err
	}
	pullPlayers["pull-bootstrap"] = bootstrap
	type cohort struct {
		name    string
		host    *harnessTypeAlias
		players map[string]*process
		watch   *playbackWatch
		seconds int
		joined  bool
	}
	cohorts := []*cohort{{name: "source", host: source, players: sourcePlayers, seconds: 36}, {name: "static-pull", host: harness, players: pullPlayers, seconds: 36}}
	continuity := map[string]any{}
	harness.results["relay_continuity"] = continuity
	defer func() {
		for _, group := range cohorts {
			if group.watch != nil && !group.joined {
				result = errors.Join(result, group.watch.finish(group.host))
				continuity[group.name] = group.host.results["playback_continuity"]
			}
		}
	}()
	for _, group := range cohorts {
		group.watch = group.host.watchPlayback(group.players)
	}
	finishCohort := func(group *cohort) error {
		if err := group.host.finishPlayers(group.players, group.seconds*25*9/10); err != nil {
			return err
		}
		err := group.watch.finish(group.host)
		continuity[group.name] = group.host.results["playback_continuity"]
		group.joined = true
		return err
	}
	for cycle := 0; cycle < 2; cycle++ {
		seconds := 6
		if cycle == 1 {
			seconds = 8
			if err := harness.relayConcurrent("start", pullURL, pushURL); err != nil {
				return err
			}
			if _, err := harness.waitPlaylist("dynamic"); err != nil {
				return err
			}
		}
		players, err := harness.players("dynamic", fmt.Sprintf("dynamic-%d-", cycle), seconds)
		if err != nil {
			return err
		}
		group := &cohort{name: fmt.Sprintf("dynamic-%d", cycle), host: harness, players: players, seconds: seconds}
		group.watch = harness.watchPlayback(players)
		cohorts = append(cohorts, group)
		if err := finishCohort(group); err != nil {
			return err
		}
		before, err := source.relayStat("source", "pub")
		if err != nil || before.Count != 1 {
			return fmt.Errorf("source publisher missing before stop: %+v: %w", before, err)
		}
		harness.results[fmt.Sprintf("source_publisher_at_dynamic_stop_%d", cycle)] = before
		if err := harness.relayConcurrent("stop", pullURL, pushURL); err != nil {
			return err
		}
		if err := harness.until("dynamic publisher stopped", 3*time.Second, func() (bool, error) {
			report, err := harness.relayStat("dynamic", "pub")
			return report.Count == 0, err
		}); err != nil {
			return err
		}
		if err := harness.waitRelayPlaylistStopped("dynamic"); err != nil {
			return err
		}
		harness.results[fmt.Sprintf("dynamic_cycle_%d_hls_retired", cycle)] = true
		harness.results[fmt.Sprintf("dynamic_cycle_%d_concurrent_start_stop", cycle)] = 16
	}
	if err := harness.until("static push retries recovered target", 38*time.Second, func() (bool, error) {
		report, err := harness.relayStat("static", "pub")
		return report.Count == 1, err
	}); err != nil {
		return err
	}
	harness.results["static_push_recovery_seconds"] = time.Since(source.started).Seconds()
	if _, err := harness.waitPlaylist("static"); err != nil {
		return err
	}
	staticPlayers, err := harness.players("static", "static-", 8)
	if err != nil {
		return err
	}
	staticGroup := &cohort{name: "static-push-recovery", host: harness, players: staticPlayers, seconds: 8}
	staticGroup.watch = harness.watchPlayback(staticPlayers)
	cohorts = append(cohorts, staticGroup)
	for _, group := range cohorts {
		if !group.joined {
			if err := finishCohort(group); err != nil {
				return err
			}
		}
	}
	if err := source.finish("publish-source", publisher, 75*time.Second); err != nil {
		return err
	}
	if err := source.stopServer(); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, app := range []string{"static", "pull", "dynamic"} {
		if err := harness.recordings(filepath.Join("recordings", app), 2, false); err != nil {
			return err
		}
	}
	if err := source.checkLogs("play-*.log", "publish-*.log"); err != nil {
		return err
	}
	if err := harness.checkLogs("play-*.log", "publish-*.log"); err != nil {
		return err
	}
	for _, host := range []*harnessTypeAlias{source, harness} {
		body, err := os.ReadFile(filepath.Join(host.evidence, "server.log"))
		if err != nil {
			return err
		}
		counts, err := validateRelayServiceLog(string(body), host == source)
		if err != nil {
			return err
		}
		host.results["relay_server_event_counts"] = counts
	}
	harness.results["relay_media_verified"] = true
	return nil
}

type harnessTypeAlias = harness

func validateRelayServiceLog(body string, source bool) (map[string]int, error) {
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
			return counts, fmt.Errorf("invalid relay server log: %w", err)
		}
		if event.Level == "ERROR" {
			if !source || !strings.HasPrefix(event.Message, "dial relay pusher: Dial rtmp server failed,") || !strings.Contains(event.Message, "connection refused") {
				return counts, fmt.Errorf("unexpected relay server error: %s", event.Message)
			}
			counts["expected_initial_dial_failure"]++
		}
		for _, app := range []string{"source", "dynamic", "static", "pull"} {
			if strings.HasPrefix(event.Message, "["+app+"/sample]New PUBLISH from ") || strings.HasPrefix(event.Message, "["+app+"/sample]Origin attached from ") {
				counts[app]++
			}
		}
	}
	expected := map[string]int{"dynamic": 2, "static": 1, "pull": 1}
	if source {
		expected = map[string]int{"source": 1, "expected_initial_dial_failure": 1}
	}
	for _, name := range []string{"source", "dynamic", "static", "pull", "expected_initial_dial_failure"} {
		if counts[name] != expected[name] {
			return counts, fmt.Errorf("relay %s count=%d, expected=%d", name, counts[name], expected[name])
		}
	}
	return counts, nil
}
