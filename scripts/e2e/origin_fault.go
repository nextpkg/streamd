package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/config"
)

func (harness *harness) originFault() (result error) {
	harness.results["publish_seconds"] = 85
	registry, err := harness.startRedisContainer("origin-fault")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, registry.close(harness)) }()
	defer harness.cleanup()
	source, err := harness.originSource("source-fault")
	if err != nil {
		return err
	}
	defer source.cleanup()
	var proxies []*originProxy
	defer func() {
		for _, proxy := range proxies {
			proxy.close()
		}
	}()
	for _, mode := range []uint32{originProxyStall, originProxyReject, originProxyPass} {
		proxy, err := newOriginProxy(harness.ctx, fmt.Sprintf("127.0.0.1:%d", source.rtmpPort), false)
		if err != nil {
			return err
		}
		proxy.fault.Store(mode)
		proxies = append(proxies, proxy)
	}
	discovered, rejected, fallback := proxies[0], proxies[1], proxies[2]
	counts := func() []int64 {
		return []int64{discovered.accepted.Load(), rejected.accepted.Load(), fallback.accepted.Load()}
	}
	requireCounts := func(expected []int64) error {
		actual := counts()
		for index := range expected {
			if actual[index] != expected[index] {
				return fmt.Errorf("origin attempts %v, expected %v", actual, expected)
			}
		}
		return nil
	}
	const registryKey = "/streamd/origin/sample"
	if err := registry.client.Set(harness.ctx, registryKey, discovered.listener.Addr().String(), 180*time.Second).Err(); err != nil {
		return err
	}
	harness.settings.RTMP.EdgeAlive, harness.settings.RTMP.EdgeCheck, harness.settings.RTMP.DialTimeout = 2, 1, 3
	harness.settings.Finder = &config.Finder{Advice: fmt.Sprintf("127.0.0.1:%d", harness.rtmpPort), Expire: 180, Interval: 1, EnableRelay: true, Separator: "/",
		Redis: &config.RedisConfig{Address: registry.address, Password: redisTestPassword, DB: 13}}
	for _, app := range []string{"origin", "control"} {
		settings := appConfig{Enable: true, AppName: app, HLS: hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 2},
			Recorder: recorderConfig{Enable: true, SaveAs: "ts", SaveMode: "immediate", Segment: 4, Driver: "fs", SaveTo: filepath.Join(harness.evidence, "recordings", app)}}
		if app == "origin" {
			settings.StaticPull = []string{"rtmp://" + rejected.listener.Addr().String() + "/origin", "rtmp://" + fallback.listener.Addr().String() + "/origin"}
		}
		harness.settings.Application = append(harness.settings.Application, settings)
	}
	if err := harness.start(); err != nil {
		return err
	}
	upstreamPublisher, err := source.publish("publish-fault-source", "origin", "bframes", 85)
	if err != nil {
		return err
	}
	controlPublisher, err := harness.publish("publish-control", "control", "regular", 85)
	if err != nil {
		return err
	}
	if _, err := harness.waitPlaylist("control"); err != nil {
		return err
	}
	controlPlayers, err := harness.pacedOriginPlayers("control", "control-", 78)
	if err != nil {
		return err
	}
	continuity := map[string]any{}
	harness.results["origin_fault_continuity"] = continuity
	var groups []*originCohort
	startGroup := func(name string, players map[string]*process) *originCohort {
		group := &originCohort{name: name, players: players, watch: harness.watchPlayback(players)}
		groups = append(groups, group)
		return group
	}
	defer func() {
		for _, group := range groups {
			if !group.finished {
				result = errors.Join(result, group.watch.finish(harness))
				continuity[group.name] = harness.results["playback_continuity"]
			}
		}
	}()
	finishGroup := func(group *originCohort, seconds int) error {
		if err := harness.finishPlayers(group.players, seconds*25*9/10); err != nil {
			return err
		}
		err := group.watch.finish(harness)
		group.finished = true
		continuity[group.name] = harness.results["playback_continuity"]
		return err
	}
	control := startGroup("unaffected-control", controlPlayers)
	coldPlayers := map[string]*process{}
	for index := 0; index < 16; index++ {
		name := fmt.Sprintf("fault-cold-%02d-http-flv", index)
		player, err := harness.originViewer(name, harness.origin+"/hfs/origin/sample.flv", 6)
		if err != nil {
			return err
		}
		coldPlayers[name] = player
	}
	cold := startGroup("http-cold-fallback", coldPlayers)
	if err := harness.until("one stalled discovery connection", 2*time.Second, func() (bool, error) { return discovered.accepted.Load() == 1, nil }); err != nil {
		return err
	}
	if err := requireCounts([]int64{1, 0, 0}); err != nil {
		return err
	}
	for name, player := range coldPlayers {
		if !player.alive() {
			return fmt.Errorf("cold HTTP viewer %s did not wait for fallback", name)
		}
	}
	harness.results["concurrent_http_cold"] = map[string]any{"viewers": 16, "all_waited": true, "attempts_while_stalled": counts()}
	if err := harness.until("ordered fallback attached", 7*time.Second, func() (bool, error) { return fallback.active.Load() == 1, nil }); err != nil {
		return err
	}
	failedAt := time.Now()
	if err := requireCounts([]int64{1, 1, 1}); err != nil {
		return err
	}
	discoveryWait := time.Duration(rejected.firstAccept.Load() - discovered.firstAccept.Load()).Seconds()
	if discoveryWait < 1.5 || discoveryWait > 5 || fallback.firstAccept.Load() < rejected.firstAccept.Load() {
		return fmt.Errorf("fallback bypassed serial discovery timeout: discovery_wait=%.3fs", discoveryWait)
	}
	harness.results["fallback_order"] = map[string]any{"discovery_wait_seconds": discoveryWait, "failed_static_before_healthy_static": true}
	if _, err := harness.waitPlaylist("origin"); err != nil {
		return err
	}
	outputs, err := harness.pacedOriginPlayers("origin", "fallback-", 6)
	if err != nil {
		return err
	}
	outputsGroup := startGroup("fallback-outputs", outputs)
	if err := finishGroup(cold, 6); err != nil {
		return err
	}
	if err := finishGroup(outputsGroup, 6); err != nil {
		return err
	}
	harness.results["ordered_fallback"] = counts()
	interrupted := map[string]*process{}
	for protocol, address := range map[string]string{"rtmp": fmt.Sprintf("rtmp://127.0.0.1:%d/origin/sample", harness.rtmpPort), "http-flv": harness.origin + "/hfs/origin/sample.flv"} {
		name := "interrupted-" + protocol
		player, err := harness.launch(name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000", "-i", address,
			"-t", "30", "-progress", filepath.Join(harness.evidence, "progress-"+name+".txt"), "-f", "null", "-")
		if err != nil {
			return err
		}
		interrupted[name] = player
	}
	if err := harness.until("fault viewers decode before disconnect", 6*time.Second, func() (bool, error) {
		for name, player := range interrupted {
			body, err := os.ReadFile(filepath.Join(harness.evidence, "progress-"+name+".txt"))
			if err != nil {
				return false, err
			}
			frames, err := decodedFrames(string(body))
			if err != nil || frames < 50 || !player.alive() {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	fallback.fault.Store(originProxyReject)
	droppedAt := time.Now()
	fallback.interrupt()
	interruptions := map[string]any{}
	for name, player := range interrupted {
		code, err := player.wait(harness.ctx, 5*time.Second)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(harness.evidence, name+".log"))
		if err != nil {
			return err
		}
		progress, err := os.ReadFile(filepath.Join(harness.evidence, "progress-"+name+".txt"))
		if err != nil {
			return err
		}
		frames, err := decodedFrames(string(progress))
		if err != nil || frames < 50 || frames >= 700 {
			return fmt.Errorf("interrupted viewer %s frames=%d err=%v", name, frames, err)
		}
		interruptions[name] = map[string]any{"returncode": code, "frames": frames, "stderr": string(body), "seconds_after_drop": time.Since(droppedAt).Seconds()}
		if err := validateInterruptedOrigin(code, string(body)); err != nil {
			harness.results["injected_disconnects"] = interruptions
			return err
		}
	}
	harness.results["injected_disconnects"] = interruptions
	assertPublishers := func() error {
		for _, sample := range []struct {
			host *harnessTypeAlias
			app  string
		}{{source, "origin"}, {harness, "control"}} {
			publication, err := sample.host.relayStat(sample.app, "pub")
			if err != nil || publication.Count != 1 {
				return fmt.Errorf("origin fault interrupted independent publisher %s: %+v %v", sample.app, publication, err)
			}
		}
		return nil
	}
	if err := assertPublishers(); err != nil {
		return err
	}
	if err := harness.until("broken origin and viewers retired", 3*time.Second, func() (bool, error) {
		publication, err := harness.relayStat("origin", "pub")
		if err != nil {
			return false, err
		}
		viewers, err := harness.originOnline()
		return publication.Count == 0 && viewers == 0 && fallback.active.Load() == 0, err
	}); err != nil {
		return err
	}
	if err := harness.waitRelayPlaylistStopped("origin"); err != nil {
		return err
	}
	if err := harness.rejectedOriginRequests(1); err != nil {
		return err
	}
	if err := requireCounts([]int64{1, 1, 2}); err != nil {
		return err
	}
	for _, proxy := range proxies {
		proxy.fault.Store(originProxyPass)
	}
	if err := harness.rejectedOriginRequests(16); err != nil {
		return err
	}
	if err := requireCounts([]int64{1, 1, 2}); err != nil {
		return err
	}
	if elapsed := time.Since(failedAt); elapsed > 50*time.Second {
		return fmt.Errorf("cooldown suppression tested too late: %v", elapsed)
	}
	harness.results["cooldown_suppression"] = map[string]any{"rejected_http_requests": 17, "all_endpoints_repaired": true, "attempts": counts(), "seconds_after_initial_failure": time.Since(failedAt).Seconds()}
	if err := harness.sleep(time.Until(failedAt.Add(55 * time.Second))); err != nil {
		return err
	}
	if err := harness.rejectedOriginRequests(16); err != nil {
		return err
	}
	if err := requireCounts([]int64{1, 1, 2}); err != nil {
		return err
	}
	if err := assertPublishers(); err != nil {
		return err
	}
	harness.results["late_cooldown_suppression"] = map[string]any{"rejected_http_requests": 16, "attempts": counts(), "seconds_after_initial_failure": time.Since(failedAt).Seconds(), "both_publishers_alive": true}
	if err := harness.sleep(time.Until(failedAt.Add(61 * time.Second))); err != nil {
		return err
	}
	retryStarted := time.Now()
	bootstrap, err := harness.originViewer("retry-bootstrap-rtmp", fmt.Sprintf("rtmp://127.0.0.1:%d/origin/sample", harness.rtmpPort), 12)
	if err != nil {
		return err
	}
	retryBootstrap := startGroup("retry-bootstrap", map[string]*process{"retry-bootstrap-rtmp": bootstrap})
	if _, err := harness.waitPlaylist("origin"); err != nil {
		return err
	}
	if err := requireCounts([]int64{2, 1, 2}); err != nil {
		return err
	}
	configuration, err := harness.configuration()
	if err != nil || configuration.Revision != 1 {
		return fmt.Errorf("cooldown retry changed configuration: revision=%d err=%v", configuration.Revision, err)
	}
	harness.results["natural_cooldown_retry"] = map[string]any{"seconds_after_initial_failure": retryStarted.Sub(failedAt).Seconds(), "attempts": counts(), "configuration_revision": configuration.Revision}
	outputs, err = harness.pacedOriginPlayers("origin", "retry-", 6)
	if err != nil {
		return err
	}
	if err := finishGroup(startGroup("retry-outputs", outputs), 6); err != nil {
		return err
	}
	if err := finishGroup(retryBootstrap, 12); err != nil {
		return err
	}
	if err := finishGroup(control, 78); err != nil {
		return err
	}
	if err := source.finish("publish-fault-source", upstreamPublisher, 100*time.Second); err != nil {
		return err
	}
	if err := harness.finish("publish-control", controlPublisher, 100*time.Second); err != nil {
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
		counts, err := validateOriginFaultLog(string(body), host == harness)
		if err != nil {
			return err
		}
		host.results["origin_fault_server_counts"] = counts
	}
	value, err := registry.client.Get(harness.ctx, registryKey).Result()
	if err != nil || value != discovered.listener.Addr().String() {
		return fmt.Errorf("origin discovery entry changed: %q %v", value, err)
	}
	remaining, err := registry.client.TTL(harness.ctx, registryKey).Result()
	expectedRemaining := 180*time.Second - time.Since(harness.started)
	if err != nil || remaining <= 0 || remaining > expectedRemaining+2*time.Second || remaining < expectedRemaining-5*time.Second {
		return fmt.Errorf("origin discovery TTL was refreshed or lost: remaining=%v expected=%v err=%v", remaining, expectedRemaining, err)
	}
	harness.results["origin_registration_preserved"] = map[string]any{"value_unchanged": true, "remaining_ttl_seconds": remaining.Seconds(), "expected_ttl_seconds": expectedRemaining.Seconds(), "control_publication_has_separate_registration": true}
	commands, err := registry.client.Info(harness.ctx, "commandstats").Result()
	if err != nil {
		return err
	}
	harness.results["redis_registration_mutations"] = map[string]int{"set": redisCommandCalls(commands, "set"), "expire": redisCommandCalls(commands, "expire"), "del": redisCommandCalls(commands, "del")}
	if err := harness.recordings("recordings", 12, true); err != nil {
		return err
	}
	harness.results["origin_fault_verified"] = true
	return nil
}

func (harness *harness) rejectedOriginRequests(count int) error {
	var workers sync.WaitGroup
	failures := make(chan error, count)
	for index := 0; index < count; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			status, body, err := harness.fetch("/hfs/origin/sample.flv")
			if err != nil || status != http.StatusOK || len(body) != 0 {
				failures <- fmt.Errorf("failed origin response HTTP %d length=%d err=%v", status, len(body), err)
			}
		}()
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		return err
	}
	publication, err := harness.relayStat("origin", "pub")
	if err != nil || publication.Count != 0 {
		return fmt.Errorf("failed origins left a phantom publication: %+v %v", publication, err)
	}
	return nil
}

func validateInterruptedOrigin(code int, body string) error {
	if code == 0 && strings.TrimSpace(body) == "" {
		return nil
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	expected := []string{"Error during demuxing: Input/output error", "Task finished with error code: -5 (Input/output error)", "Terminating thread with return code -5 (Input/output error)"}
	if code == 251 && len(lines) == len(expected) {
		prefix := regexp.MustCompile(`^\[in#0/flv @ 0x[0-9a-fA-F]+\] `)
		valid := true
		for index, line := range lines {
			if !prefix.MatchString(line) || prefix.ReplaceAllString(line, "") != expected[index] {
				valid = false
			}
		}
		if valid {
			return nil
		}
	}
	return fmt.Errorf("unexpected interrupted-origin FFmpeg exit=%d stderr=%q", code, body)
}

func (harness *harness) pacedOriginPlayers(app, prefix string, seconds int) (map[string]*process, error) {
	players := map[string]*process{}
	for protocol, address := range map[string]string{"rtmp": fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app), "http-flv": harness.origin + "/hfs/" + app + "/sample.flv", "hls": harness.origin + "/hls/" + app + "/sample.m3u8"} {
		name := prefix + protocol
		player, err := harness.launch("play-"+name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000", "-re", "-i", address, "-t", fmt.Sprint(seconds), "-progress", filepath.Join(harness.evidence, "progress-"+name+".txt"), "-f", "null", "-")
		if err != nil {
			return nil, err
		}
		players[name] = player
	}
	return players, nil
}

func validateOriginFaultLog(body string, edge bool) (map[string]int, error) {
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
			return counts, fmt.Errorf("unexpected origin-fault server error: %s", event.Message)
		}
		for name, prefix := range map[string]string{"publish": "[origin/sample]New PUBLISH from ", "origin": "[origin/sample]Origin attached from ", "play": "[origin/sample]New PLAY from ", "terminated": "[origin/sample]Client terminated"} {
			if strings.HasPrefix(event.Message, prefix) {
				counts[name]++
			}
		}
	}
	expected := map[string]int{"publish": 1, "play": 2, "origin": 0, "terminated": 1}
	if edge {
		expected = map[string]int{"publish": 0, "play": 4, "origin": 2, "terminated": 2}
	}
	for name, want := range expected {
		if counts[name] != want {
			return counts, fmt.Errorf("origin-fault %s count=%d want=%d", name, counts[name], want)
		}
	}
	return counts, nil
}
