package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func validateStaticPushLog(body string, source bool) (map[string]int, error) {
	counts := map[string]int{}
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event struct{ Level, Msg string }
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, err
		}
		if event.Level == "WARN" {
			if event.Msg != "Not continuous timestamp" {
				return nil, fmt.Errorf("unexpected static-push warning: %s", event.Msg)
			}
			counts["retained_timestamp_warnings"]++
		}
		if event.Level == "ERROR" {
			if !source || !strings.HasPrefix(event.Msg, "start relay pusher: client handshake to server: receive S0S1S2: read failed,read=0:") || !strings.HasSuffix(event.Msg, "i/o timeout") {
				return nil, fmt.Errorf("unexpected static-push error: %s", event.Msg)
			}
			counts["expected_initial_handshake_timeout"]++
		}
		for _, app := range []string{"origin", "control", "healthy", "unstable", "stalled"} {
			if strings.HasPrefix(event.Msg, "["+app+"/sample]New PUBLISH from ") {
				counts[app]++
			}
		}
	}
	expected := map[string]int{"control": 1, "healthy": 2, "unstable": 3, "stalled": 1}
	if source {
		expected = map[string]int{"origin": 2, "expected_initial_handshake_timeout": 1}
	}
	for _, key := range []string{"origin", "control", "healthy", "unstable", "stalled", "expected_initial_handshake_timeout"} {
		if counts[key] != expected[key] {
			return nil, fmt.Errorf("static-push event %s=%d expected=%d", key, counts[key], expected[key])
		}
	}
	return counts, nil
}

func (harness *harness) staticPushMedia() (result error) {
	for _, app := range []string{"control", "healthy", "unstable", "stalled"} {
		format := "ts"
		if app == "healthy" || app == "stalled" {
			format = "flv"
		}
		harness.settings.Application = append(harness.settings.Application, appConfig{Enable: true, AppName: app,
			HLS:      hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 2},
			Recorder: recorderConfig{Enable: true, SaveAs: format, SaveMode: "immediate", Segment: 4, Driver: "fs", SaveTo: filepath.Join(harness.evidence, "recordings")}})
	}
	if err := harness.start(); err != nil {
		return err
	}
	proxies := map[string]*originProxy{}
	for _, app := range []string{"stalled", "healthy", "unstable"} {
		proxy, err := newOriginProxy(harness.ctx, fmt.Sprintf("127.0.0.1:%d", harness.rtmpPort), false)
		if err != nil {
			return err
		}
		defer proxy.close()
		if app == "stalled" {
			proxy.fault.Store(originProxyStall)
		}
		proxies[app] = proxy
	}
	source, err := harness.originSource("source-static")
	if err != nil {
		return err
	}
	defer source.cleanup()
	source.settings.RTMP.DialTimeout = 3
	app := &source.settings.Application[0]
	app.HLS = hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 2}
	app.Recorder = recorderConfig{Enable: true, SaveAs: "flv", SaveMode: "immediate", Segment: 4, Driver: "fs", SaveTo: filepath.Join(source.evidence, "recordings")}
	for _, target := range []string{"stalled", "healthy", "unstable"} {
		app.StaticPush = append(app.StaticPush, "rtmp://"+proxies[target].listener.Addr().String()+"/"+target)
	}
	if err := source.commit("configure-three-static-targets", nil); err != nil {
		return err
	}
	publisher, err := source.publish("publish-source", "origin", "regular", 60)
	if err != nil {
		return err
	}
	published := time.Now()
	controlPublisher, err := harness.publish("publish-control", "control", "regular", 60)
	if err != nil {
		return err
	}
	if err := harness.until("healthy targets publishing before sibling timeout", time.Second, func() (bool, error) {
		for _, name := range []string{"healthy", "unstable"} {
			report, err := harness.relayStat(name, "pub")
			if err != nil || report.Count != 1 {
				return false, err
			}
		}
		return proxies["stalled"].accepted.Load() == 1, nil
	}); err != nil {
		return err
	}
	harness.results["healthy_targets_started_before_stall_timeout_seconds"] = time.Since(published).Seconds()
	type cohort struct {
		host    *harnessTypeAlias
		name    string
		players map[string]*process
		watch   *playbackWatch
		joined  bool
	}
	groups := []*cohort{}
	continuity := map[string]any{}
	harness.results["static_push_continuity"] = continuity
	defer func() {
		for _, group := range groups {
			if !group.joined {
				result = errors.Join(result, group.watch.finish(group.host))
				continuity[group.name] = group.host.results["playback_continuity"]
			}
		}
	}()
	for _, node := range []struct {
		host *harnessTypeAlias
		app  string
	}{{source, "origin"}, {harness, "control"}, {harness, "healthy"}} {
		if _, err := node.host.waitPlaylist(node.app); err != nil {
			return err
		}
		players, err := node.host.players(node.app, node.app+"-", 42)
		if err != nil {
			return err
		}
		groups = append(groups, &cohort{host: node.host, name: node.app, players: players, watch: node.host.watchPlayback(players)})
	}
	if _, err := harness.waitPlaylist("unstable"); err != nil {
		return err
	}
	initialPlayers, err := harness.players("unstable", "initial-unstable-", 6)
	if err != nil {
		return err
	}
	if err := harness.finishPlayers(initialPlayers, 140); err != nil {
		return err
	}
	interrupted := map[string]*process{}
	for protocol, address := range map[string]string{"rtmp": fmt.Sprintf("rtmp://127.0.0.1:%d/unstable/sample", harness.rtmpPort), "http-flv": harness.origin + "/hfs/unstable/sample.flv"} {
		name := "interrupted-" + protocol
		child, err := harness.launch(name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000", "-i", address,
			"-t", "30", "-progress", filepath.Join(harness.evidence, "progress-"+name+".txt"), "-f", "null", "-")
		if err != nil {
			return err
		}
		interrupted[name] = child
	}
	if err := harness.until("affected target viewers decoding", 6*time.Second, func() (bool, error) {
		for name, child := range interrupted {
			body, err := os.ReadFile(filepath.Join(harness.evidence, "progress-"+name+".txt"))
			if err != nil {
				return false, err
			}
			frames, err := decodedFrames(string(body))
			if err != nil || frames < 50 || !child.alive() {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	proxies["unstable"].fault.Store(originProxyReject)
	dropped := time.Now()
	proxies["unstable"].interrupt()
	interruptions := map[string]any{}
	harness.results["planned_target_interruptions"] = interruptions
	for name, child := range interrupted {
		code, err := child.wait(harness.ctx, 5*time.Second)
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
			return fmt.Errorf("affected viewer %s frames=%d error=%v", name, frames, err)
		}
		if err := validateInterruptedOrigin(code, string(body)); err != nil {
			return err
		}
		interruptions[name] = map[string]any{"returncode": code, "frames": frames, "stderr": string(body), "seconds_after_drop": time.Since(dropped).Seconds()}
	}
	if err := harness.until("only faulty target retired", 3*time.Second, func() (bool, error) {
		report, err := harness.relayStat("unstable", "pub")
		return report.Count == 0 && proxies["unstable"].active.Load() == 0, err
	}); err != nil {
		return err
	}
	if err := harness.waitRelayPlaylistStopped("unstable"); err != nil {
		return err
	}
	for _, name := range []string{"stalled", "unstable"} {
		proxies[name].fault.Store(originProxyPass)
	}
	if time.Since(published) > 24*time.Second {
		return errors.New("fault injection finished too late to verify original retry cadence")
	}
	if err := harness.sleep(2 * time.Second); err != nil {
		return err
	}
	for _, proxy := range proxies {
		if proxy.accepted.Load() != 1 {
			return errors.New("static target retried before the original thirty-second cadence")
		}
	}
	if err := harness.until("faulty targets recover on retry", 20*time.Second, func() (bool, error) {
		for _, name := range []string{"stalled", "unstable"} {
			report, err := harness.relayStat(name, "pub")
			if err != nil || report.Count != 1 || proxies[name].accepted.Load() != 2 {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	recovered := time.Since(published).Seconds()
	if recovered < 29 || recovered > 33 || proxies["healthy"].accepted.Load() != 1 || proxies["healthy"].active.Load() != 1 {
		return fmt.Errorf("recovery cadence or healthy target isolation changed: seconds=%.3f healthy=%d", recovered, proxies["healthy"].accepted.Load())
	}
	harness.results["static_target_recovery"] = map[string]any{"seconds_from_publish": recovered, "healthy_connections": 1, "unstable_connections": 2, "stalled_connections": 2}
	recoveredPlayers := map[string]*process{}
	for _, app := range []string{"stalled", "unstable"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
		players, err := harness.players(app, "recovered-"+app+"-", 12)
		if err != nil {
			return err
		}
		maps.Copy(recoveredPlayers, players)
	}
	if err := harness.finishPlayers(recoveredPlayers, 275); err != nil {
		return err
	}
	for _, group := range groups {
		if err := group.host.finishPlayers(group.players, 1000); err != nil {
			return err
		}
		watchError := group.watch.finish(group.host)
		group.joined = true
		continuity[group.name] = group.host.results["playback_continuity"]
		if watchError != nil {
			return watchError
		}
	}
	if err := source.finish("publish-source", publisher, 65*time.Second); err != nil {
		return err
	}
	if err := harness.finish("publish-control", controlPublisher, 65*time.Second); err != nil {
		return err
	}
	if err := harness.until("natural publisher exit retires every static target", 3*time.Second, func() (bool, error) {
		for _, proxy := range proxies {
			if proxy.active.Load() != 0 {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	proxies["stalled"].fault.Store(originProxyStall)
	shutdownPublisher, err := source.publish("shutdown-publisher", "origin", "regular", 30)
	if err != nil {
		return err
	}
	if err := harness.until("shutdown with active targets and pending handshake", time.Second, func() (bool, error) {
		for _, name := range []string{"healthy", "unstable"} {
			report, err := harness.relayStat(name, "pub")
			if err != nil || report.Count != 1 || len(report.List) != 1 || report.List[0].VideoReceived == 0 {
				return false, err
			}
		}
		return proxies["stalled"].accepted.Load() == 3 && proxies["stalled"].active.Load() == 1, nil
	}); err != nil {
		return err
	}
	if err := harness.sleep(300 * time.Millisecond); err != nil {
		return err
	}
	shutdown := time.Now()
	if err := source.stopServer(); err != nil {
		return err
	}
	if elapsed := time.Since(shutdown).Seconds(); elapsed > 3 {
		return fmt.Errorf("static-target shutdown exceeded budget: %.3fs", elapsed)
	}
	code, err := shutdownPublisher.wait(harness.ctx, 3*time.Second)
	if err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(source.evidence, "shutdown-publisher.log"))
	if err != nil {
		return err
	}
	if err := validateShutdownPublisher(code, string(body)); err != nil {
		return err
	}
	if err := harness.until("all target TCP owners retired after source stop", time.Second, func() (bool, error) {
		for _, proxy := range proxies {
			if proxy.active.Load() != 0 {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	harness.results["static_shutdown"] = map[string]any{"seconds": time.Since(shutdown).Seconds(), "publisher_expected_exit": code, "target_connections_remaining": 0, "pending_handshake_cancelled": true}
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, host := range []*harnessTypeAlias{source, harness} {
		if err := host.recordings("recordings", 10, true); err != nil {
			return err
		}
		if err := host.checkLogs("play-*.log", "publish-*.log"); err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(host.evidence, "server.log"))
		if err != nil {
			return err
		}
		counts, err := validateStaticPushLog(string(body), host == source)
		if err != nil {
			return err
		}
		host.results["static_push_events"] = counts
	}
	harness.results["static_push_media_verified"] = true
	return nil
}
