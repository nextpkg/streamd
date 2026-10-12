package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nextpkg/streamd/config"
)

type originCohort struct {
	name     string
	players  map[string]*process
	watch    *playbackWatch
	finished bool
}

func (harness *harness) originSource(label string) (*harnessTypeAlias, error) {
	rtmpPort, err := freePort()
	if err != nil {
		return nil, err
	}
	httpPort, err := freePort()
	if err != nil {
		return nil, err
	}
	source := &harnessTypeAlias{ctx: harness.ctx, root: harness.root, evidence: filepath.Join(harness.evidence, label), rtmpPort: rtmpPort,
		origin: fmt.Sprintf("http://127.0.0.1:%d", httpPort), httpClient: &http.Client{Timeout: 2 * time.Second}, results: map[string]any{}}
	if err := os.MkdirAll(source.evidence, 0700); err != nil {
		return nil, err
	}
	source.settings.HTTP.Addr = fmt.Sprintf("127.0.0.1:%d", httpPort)
	source.settings.RTMP.Addr = fmt.Sprintf("127.0.0.1:%d", rtmpPort)
	source.settings.Application = []appConfig{{Enable: true, AppName: "origin"}}
	filename := filepath.Join(source.evidence, "config.json")
	if err := writeJSON(filename, source.settings); err != nil {
		return nil, err
	}
	source.server, err = source.launch("server", filepath.Join(harness.evidence, "streamd"), "-config", filename)
	if err != nil {
		return nil, err
	}
	if err := source.until(label+" readiness", 10*time.Second, func() (bool, error) {
		status, _, err := source.fetch("/readyz")
		return status == http.StatusOK, err
	}); err != nil {
		source.cleanup()
		return nil, err
	}
	source.started = time.Now()
	harness.results[label] = source.results
	return source, nil
}

func (harness *harness) originViewer(name, address string, seconds int) (*process, error) {
	return harness.launch("play-"+name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000",
		"-i", address, "-t", strconv.Itoa(seconds), "-progress", filepath.Join(harness.evidence, "progress-"+name+".txt"), "-f", "null", "-")
}

func (harness *harness) originOnline() (int, error) {
	status, body, err := harness.fetch("/admin/stat/sub/online?key=origin/sample")
	var envelope struct {
		Code int `json:"code"`
		Data struct {
			Count int `json:"count"`
		} `json:"data"`
	}
	if err == nil {
		err = json.Unmarshal(body, &envelope)
	}
	if err != nil || status != http.StatusOK || envelope.Code != 0 {
		return 0, fmt.Errorf("origin viewer count HTTP %d: %s: %w", status, body, err)
	}
	return envelope.Data.Count, nil
}

func (harness *harness) originPlayers(prefix string, observation *hlsObservation) (map[string]*process, error) {
	players := map[string]*process{}
	for protocol, address := range map[string]string{
		"rtmp":     fmt.Sprintf("rtmp://127.0.0.1:%d/origin/sample", harness.rtmpPort),
		"http-flv": harness.origin + "/hfs/origin/sample.flv",
		"hls":      observation.server.URL + "/hls/origin/sample.m3u8",
	} {
		name := prefix + protocol
		player, err := harness.originViewer(name, address, 6)
		if err != nil {
			return nil, err
		}
		players[name] = player
	}
	return players, nil
}

func (harness *harness) originIdle(label string, proxy *originProxy, source *harnessTypeAlias, observation *hlsObservation) error {
	if err := harness.until(label+" external viewers disconnected", 3*time.Second, func() (bool, error) {
		count, err := harness.originOnline()
		return count == 0, err
	}); err != nil {
		return err
	}
	started := time.Now()
	hlsDeadline := time.Unix(0, observation.deadline.Load())
	expectedDeadline := max(started.Add(7*time.Second).UnixNano(), hlsDeadline.UnixNano())
	if proxy.active.Load() != 1 {
		return errors.New("origin disappeared before its idle retention window")
	}
	if err := harness.until(label+" idle origin released", time.Until(time.Unix(0, expectedDeadline))+3*time.Second, func() (bool, error) {
		report, err := harness.relayStat("origin", "pub")
		return report.Count == 0 && proxy.active.Load() == 0, err
	}); err != nil {
		return err
	}
	delay := time.Since(started).Seconds()
	hlsDelay := time.Since(hlsDeadline).Seconds()
	if delay < 3.5 || hlsDelay < -0.5 || time.Since(time.Unix(0, expectedDeadline)) > 3*time.Second {
		return fmt.Errorf("origin eviction violated stateful or observed HLS window: stateful=%.3fs HLS=%.3fs", delay, hlsDelay)
	}
	if err := harness.waitRelayPlaylistStopped("origin"); err != nil {
		return err
	}
	if err := source.until(label+" upstream viewer released", 3*time.Second, func() (bool, error) {
		count, err := source.originOnline()
		return count == 0, err
	}); err != nil {
		return err
	}
	report, err := source.relayStat("origin", "pub")
	if err != nil || report.Count != 1 {
		return fmt.Errorf("idle origin cleanup interrupted the source publisher: %+v: %w", report, err)
	}
	harness.results["idle_"+label] = map[string]any{"edge_alive_seconds": 6, "edge_check_seconds": 1, "elapsed_after_last_viewer_seconds": delay,
		"observed_hls_window_seconds": time.Duration(observation.window.Load()).Seconds(), "seconds_after_hls_deadline": hlsDelay,
		"expected_max_after_last_viewer_seconds": time.Unix(0, expectedDeadline).Sub(started).Seconds() + 3,
		"upstream_tcp_connections":               proxy.active.Load(), "source_publisher_alive": true}
	return nil
}

func (harness *harness) originMedia() (result error) {
	registry, err := harness.startRedisContainer("origin")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, registry.close(harness)) }()
	defer harness.cleanup()
	primary, err := harness.originSource("source-primary")
	if err != nil {
		return err
	}
	defer primary.cleanup()
	replacement, err := harness.originSource("source-replacement")
	if err != nil {
		return err
	}
	defer replacement.cleanup()
	primaryProxy, err := newOriginProxy(harness.ctx, fmt.Sprintf("127.0.0.1:%d", primary.rtmpPort), true)
	if err != nil {
		return err
	}
	defer primaryProxy.close()
	replacementProxy, err := newOriginProxy(harness.ctx, fmt.Sprintf("127.0.0.1:%d", replacement.rtmpPort), false)
	if err != nil {
		return err
	}
	defer replacementProxy.close()
	primaryAddress, replacementAddress := primaryProxy.listener.Addr().String(), replacementProxy.listener.Addr().String()
	const registryKey = "/streamd/origin/sample"
	if err := registry.client.Set(harness.ctx, registryKey, primaryAddress, 120*time.Second).Err(); err != nil {
		return err
	}
	harness.settings.RTMP.EdgeAlive, harness.settings.RTMP.EdgeCheck, harness.settings.RTMP.DialTimeout = 6, 1, 8
	harness.settings.Finder = &config.Finder{Advice: fmt.Sprintf("127.0.0.1:%d", harness.rtmpPort), Expire: 120, Interval: 1, EnableRelay: true, Separator: "/",
		Redis: &config.RedisConfig{Address: registry.address, Password: redisTestPassword, DB: 13}}
	harness.settings.Application = []appConfig{{Enable: true, AppName: "origin", StaticPull: []string{"rtmp://" + replacementAddress + "/origin"},
		HLS:      hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 3},
		Recorder: recorderConfig{Enable: true, SaveAs: "ts", SaveMode: "immediate", Segment: 4, Driver: "fs", SaveTo: filepath.Join(harness.evidence, "recordings")}}}
	if err := harness.start(); err != nil {
		return err
	}
	observation, err := observeHLS(harness.origin, 6*time.Second)
	if err != nil {
		return err
	}
	defer observation.server.Close()
	primaryPublisher, err := primary.publish("publish-primary", "origin", "regular", 60)
	if err != nil {
		return err
	}
	replacementPublisher, err := replacement.publish("publish-replacement", "origin", "bframes", 60)
	if err != nil {
		return err
	}
	for _, source := range []*harnessTypeAlias{primary, replacement} {
		if err := source.until("source publisher active", 3*time.Second, func() (bool, error) {
			report, err := source.relayStat("origin", "pub")
			return report.Count == 1, err
		}); err != nil {
			return err
		}
	}
	continuity := map[string]any{}
	harness.results["origin_continuity"] = continuity
	var cohorts []*originCohort
	startCohort := func(name string, players map[string]*process) *originCohort {
		group := &originCohort{name: name, players: players, watch: harness.watchPlayback(players)}
		cohorts = append(cohorts, group)
		return group
	}
	defer func() {
		for _, group := range cohorts {
			if !group.finished {
				result = errors.Join(result, group.watch.finish(harness))
				continuity[group.name] = harness.results["playback_continuity"]
			}
		}
	}()
	finishCohort := func(group *originCohort, seconds int) error {
		if err := harness.finishPlayers(group.players, seconds*25*9/10); err != nil {
			return err
		}
		err := group.watch.finish(harness)
		group.finished = true
		continuity[group.name] = harness.results["playback_continuity"]
		return err
	}
	address := fmt.Sprintf("rtmp://127.0.0.1:%d/origin/sample", harness.rtmpPort)
	coldPlayers := map[string]*process{}
	for viewer := 0; viewer < 16; viewer++ {
		name := fmt.Sprintf("cold-%02d-rtmp", viewer)
		player, err := harness.originViewer(name, address, 12)
		if err != nil {
			return err
		}
		coldPlayers[name] = player
	}
	cold := startCohort("cold", coldPlayers)
	if err := harness.until("sixteen viewers waiting on one blocked origin", 4*time.Second, func() (bool, error) {
		body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
		return strings.Count(string(body), "[origin/sample]New PLAY from ") == 16 && primaryProxy.accepted.Load() == 1, err
	}); err != nil {
		return err
	}
	if err := harness.sleep(time.Second); err != nil {
		return err
	}
	for name, player := range coldPlayers {
		if !player.alive() {
			return fmt.Errorf("cold viewer %s rejected before origin became ready", name)
		}
	}
	if primaryProxy.accepted.Load() != 1 || replacementProxy.accepted.Load() != 0 {
		return errors.New("cold viewers opened duplicate origins or bypassed discovery priority")
	}
	harness.results["blocked_cold_start"] = map[string]any{"viewers": 16, "primary_connections": 1, "static_connections": 0, "all_waited": true}
	primaryProxy.open()
	if _, err := harness.waitPlaylist("origin"); err != nil {
		return err
	}
	players, err := harness.originPlayers("primary-", observation)
	if err != nil {
		return err
	}
	primaryOutputs := startCohort("primary-outputs", players)
	before, err := harness.relayStat("origin", "pub")
	if err != nil || before.Count != 1 || len(before.List) != 1 {
		return fmt.Errorf("missing primary origin: %+v: %w", before, err)
	}
	if err := registry.client.Set(harness.ctx, registryKey, replacementAddress, 120*time.Second).Err(); err != nil {
		return err
	}
	replacementRegisteredAt := time.Now()
	harness.settings.Application[0].StaticPull = []string{"rtmp://" + primaryAddress + "/origin"}
	if err := harness.atomicSettings(); err != nil {
		return err
	}
	if err := harness.until("source policy revision committed", 5*time.Second, func() (bool, error) {
		report, err := harness.configuration()
		return report.Revision == 2, err
	}); err != nil {
		return err
	}
	if err := harness.sleep(time.Second); err != nil {
		return err
	}
	after, err := harness.relayStat("origin", "pub")
	if err != nil || after.Count != 1 || len(after.List) != 1 || after.List[0].VideoReceived <= before.List[0].VideoReceived || after.List[0].Duration < before.List[0].Duration || primaryProxy.accepted.Load() != 1 || replacementProxy.accepted.Load() != 0 {
		return fmt.Errorf("source policy reload replaced or stalled the healthy origin: before=%+v after=%+v err=%v", before, after, err)
	}
	harness.results["healthy_origin_retained_across_policy_reload"] = map[string]any{"revision": 2, "before": before, "after": after}
	if err := finishCohort(primaryOutputs, 6); err != nil {
		return err
	}
	if err := finishCohort(cold, 12); err != nil {
		return err
	}
	if err := harness.until("cold viewers gone", 3*time.Second, func() (bool, error) { count, err := harness.originOnline(); return count == 0, err }); err != nil {
		return err
	}
	if err := harness.sleep(time.Second); err != nil {
		return err
	}
	warmPlayers := map[string]*process{}
	for protocol, location := range map[string]string{"rtmp": address, "http-flv": harness.origin + "/hfs/origin/sample.flv"} {
		name := "warm-" + protocol
		player, err := harness.originViewer(name, location, 4)
		if err != nil {
			return err
		}
		warmPlayers[name] = player
	}
	if err := finishCohort(startCohort("warm-reuse", warmPlayers), 4); err != nil {
		return err
	}
	if primaryProxy.accepted.Load() != 1 || replacementProxy.accepted.Load() != 0 {
		return errors.New("warm viewers did not reuse the healthy origin")
	}
	harness.results["warm_reuse_connections"] = map[string]int64{"primary": primaryProxy.accepted.Load(), "replacement": replacementProxy.accepted.Load()}
	if err := harness.originIdle("primary", primaryProxy, primary, observation); err != nil {
		return err
	}
	bootstrap, err := harness.originViewer("replacement-bootstrap-http-flv", harness.origin+"/hfs/origin/sample.flv", 12)
	if err != nil {
		return err
	}
	bootstrapGroup := startCohort("replacement-http-cold", map[string]*process{"replacement-bootstrap-http-flv": bootstrap})
	if _, err := harness.waitPlaylist("origin"); err != nil {
		return err
	}
	players, err = harness.originPlayers("replacement-", observation)
	if err != nil {
		return err
	}
	if err := finishCohort(startCohort("replacement-outputs", players), 6); err != nil {
		return err
	}
	if err := finishCohort(bootstrapGroup, 12); err != nil {
		return err
	}
	if primaryProxy.accepted.Load() != 1 || replacementProxy.accepted.Load() != 1 {
		return errors.New("next cold origin did not apply the new discovery-before-static policy")
	}
	harness.results["next_origin_uses_new_policy"] = true
	if err := harness.originIdle("replacement", replacementProxy, replacement, observation); err != nil {
		return err
	}
	if err := primary.finish("publish-primary", primaryPublisher, 75*time.Second); err != nil {
		return err
	}
	if err := replacement.finish("publish-replacement", replacementPublisher, 75*time.Second); err != nil {
		return err
	}
	for _, host := range []*harnessTypeAlias{harness, primary, replacement} {
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
		counts, err := validateOriginServiceLog(string(body), host == harness)
		if err != nil {
			return err
		}
		host.results["origin_server_event_counts"] = counts
	}
	value, err := registry.client.Get(harness.ctx, registryKey).Result()
	if err != nil || value != replacementAddress {
		return fmt.Errorf("origin lifecycle overwrote or removed the discovery registration: %q: %w", value, err)
	}
	commands, err := registry.client.Info(harness.ctx, "commandstats").Result()
	if err != nil {
		return err
	}
	mutations := map[string]int{"set": redisCommandCalls(commands, "set"), "expire": redisCommandCalls(commands, "expire"), "del": redisCommandCalls(commands, "del")}
	if mutations["set"] != 4 || mutations["expire"] != 2 || mutations["del"] != 2 {
		return fmt.Errorf("external origins mutated local publication registration: %+v", mutations)
	}
	remaining, err := registry.client.PTTL(harness.ctx, registryKey).Result()
	expectedRemaining := 120*time.Second - time.Since(replacementRegisteredAt)
	if err != nil || remaining <= 0 || remaining > expectedRemaining+time.Second || remaining < expectedRemaining-time.Second {
		return fmt.Errorf("external origin changed source TTL: got %v want %v: %v", remaining, expectedRemaining, err)
	}
	probes, err := registry.client.Keys(harness.ctx, "*__streamd_probe_*").Result()
	if err != nil || len(probes) != 0 {
		return fmt.Errorf("origin configuration left preparation probes: %v", err)
	}
	harness.results["redis_preparation_mutations"] = map[string]int{"set": 2, "expire": 2, "del": 2}
	harness.results["redis_source_ttl_preserved"] = true
	harness.results["redis_registration_mutations"] = mutations
	if err := harness.recordings("recordings", 6, true); err != nil {
		return err
	}
	harness.results["origin_media_verified"] = true
	return nil
}

func validateOriginServiceLog(body string, edge bool) (map[string]int, error) {
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
			return counts, fmt.Errorf("unexpected origin service error: %s", event.Message)
		}
		for name, prefix := range map[string]string{"publish": "[origin/sample]New PUBLISH from ", "play": "[origin/sample]New PLAY from ",
			"origin": "[origin/sample]Origin attached from ", "idle": "[origin/sample]Data source idling over "} {
			if strings.HasPrefix(event.Message, prefix) {
				counts[name]++
			}
		}
	}
	expected := map[string]int{"publish": 1, "play": 1, "origin": 0, "idle": 0}
	if edge {
		expected = map[string]int{"publish": 0, "play": 19, "origin": 2, "idle": 2}
	}
	for name, count := range expected {
		if counts[name] != count {
			return counts, fmt.Errorf("origin %s count=%d expected=%d", name, counts[name], count)
		}
	}
	return counts, nil
}
