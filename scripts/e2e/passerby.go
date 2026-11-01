package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func validatePasserbyEvents(body string) (map[string]int, int, error) {
	expected := map[string]int{"idle/sample": 1, "keep/fresh": 1, "stateful/sample": 1, "hls/sample": 1}
	idle := map[string]int{}
	warnings := 0
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event struct{ Level, Msg string }
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, warnings, err
		}
		if event.Level == "ERROR" {
			return nil, warnings, fmt.Errorf("unexpected Passerby error: %s", event.Msg)
		}
		if event.Level == "WARN" {
			if event.Msg != "Not continuous timestamp" {
				return nil, warnings, fmt.Errorf("unexpected Passerby warning: %s", event.Msg)
			}
			warnings++
		}
		if key, found := strings.CutSuffix(event.Msg, "]Data source idling over 3s."); found {
			key, valid := strings.CutPrefix(key, "[")
			if !valid || event.Level != "INFO" {
				return nil, warnings, errors.New("invalid idle retirement event")
			}
			idle[key]++
		}
	}
	if !maps.Equal(idle, expected) {
		return nil, warnings, fmt.Errorf("idle retirement events=%v expected=%v", idle, expected)
	}
	return idle, warnings, nil
}

func (harness *harness) retiredRecordingSizes() (map[string]int64, error) {
	sizes := map[string]int64{}
	for _, key := range []string{"idle/sample", "keep/fresh", "stateful/sample", "hls/sample"} {
		files, err := recordingFiles(filepath.Join(harness.evidence, "recordings", key))
		if err != nil || len(files) == 0 {
			return nil, fmt.Errorf("missing retired recordings for %s: %v", key, err)
		}
		for _, filename := range files {
			info, err := os.Stat(filename)
			if err != nil {
				return nil, err
			}
			sizes[filename] = info.Size()
		}
	}
	return sizes, nil
}

func (harness *harness) publicationCount(app, instance, kind string) (int, error) {
	data, err := harness.adminData(harness.ctx, "/admin/stat/"+kind+"/info?key="+url.QueryEscape(app+"/"+instance))
	if err != nil {
		return 0, err
	}
	count, valid := data["count"].(float64)
	if !valid || count < 0 || count != float64(int(count)) {
		return 0, errors.New("invalid publication count")
	}
	return int(count), nil
}

func (harness *harness) publishInstance(name, app, instance string, seconds int) (*process, error) {
	return harness.launch(name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-re", "-stream_loop", "-1",
		"-i", filepath.Join(harness.root, "testdata", "generated", "regular.flv"), "-t", strconv.Itoa(seconds), "-c", "copy", "-f", "flv",
		fmt.Sprintf("rtmp://127.0.0.1:%d/%s/%s", harness.rtmpPort, app, instance))
}

func (harness *harness) passerbyRetired(name, app, instance string, child *process, timeout time.Duration) (map[string]any, error) {
	started := time.Now()
	if err := harness.until("Passerby retirement "+name, timeout, func() (bool, error) {
		count, err := harness.publicationCount(app, instance, "pub")
		return count == 0 && !child.alive(), err
	}); err != nil {
		return nil, err
	}
	code, err := child.wait(harness.ctx, time.Second)
	if err != nil {
		return nil, err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, name+".log"))
	if err != nil {
		return nil, err
	}
	if err := validateShutdownPublisher(code, string(body)); err != nil {
		return nil, err
	}
	return map[string]any{"app": app, "instance": instance, "returncode": code, "wait_seconds": time.Since(started).Seconds(), "publisher_count": 0, "expected_idle_disconnect": true}, nil
}

func (harness *harness) passerbyMedia() (failure error) {
	harness.settings.RTMP.EdgeAlive, harness.settings.RTMP.EdgeCheck = 3, 1
	for _, app := range []string{"control", "keep", "idle", "stateful", "hls"} {
		format := "ts"
		if app == "keep" || app == "stateful" {
			format = "flv"
		}
		harness.settings.Application = append(harness.settings.Application, appConfig{
			Enable: true, AppName: app, Passerby: app != "control" && app != "keep",
			HLS:      hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 2},
			Recorder: recorderConfig{Enable: true, SaveAs: format, SaveMode: "immediate", Segment: 4, Driver: "fs", SaveTo: filepath.Join(harness.evidence, "recordings")},
		})
	}
	if err := harness.start(); err != nil {
		return err
	}
	publishers := map[string]*process{}
	for _, app := range harness.settings.Application {
		name := "idle-publisher-" + app.AppName
		if !app.Passerby {
			name = "publish-" + app.AppName
		}
		child, err := harness.publish(name, app.AppName, "regular", 60)
		if err != nil {
			return err
		}
		publishers[app.AppName] = child
	}
	if err := harness.until("initial local publishers", 2*time.Second, func() (bool, error) {
		for _, app := range []string{"control", "keep", "idle", "stateful", "hls"} {
			count, err := harness.publicationCount(app, "sample", "pub")
			if err != nil || count != 1 {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	idleObserved := time.Now()
	stateful := map[string]*process{}
	for protocol, address := range map[string]string{
		"rtmp":     fmt.Sprintf("rtmp://127.0.0.1:%d/stateful/sample", harness.rtmpPort),
		"http-flv": harness.origin + "/hfs/stateful/sample.flv",
	} {
		name := "stateful-" + protocol
		child, err := harness.originViewer(name, address, 24)
		if err != nil {
			return err
		}
		stateful[name] = child
	}
	bootstrap, err := harness.originViewer("hls-bootstrap", fmt.Sprintf("rtmp://127.0.0.1:%d/hls/sample", harness.rtmpPort), 8)
	if err != nil {
		return err
	}
	if _, err := harness.waitPlaylist("control"); err != nil {
		return err
	}
	control, err := harness.players("control", "control-", 46)
	if err != nil {
		return err
	}
	if _, err := harness.waitPlaylist("hls"); err != nil {
		return err
	}
	observation, err := observeHLS(harness.origin, 3*time.Second)
	if err != nil {
		return err
	}
	defer observation.server.Close()
	hlsPlayer, err := harness.launch("play-hls-only", "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000", "-re",
		"-i", observation.server.URL+"/hls/hls/sample.m3u8", "-t", "28", "-progress", filepath.Join(harness.evidence, "progress-hls-only.txt"), "-f", "null", "-")
	if err != nil {
		return err
	}
	allPlayers := map[string]*process{"hls-only": hlsPlayer, "hls-bootstrap": bootstrap}
	for name, child := range control {
		allPlayers[name] = child
	}
	for name, child := range stateful {
		allPlayers[name] = child
	}
	watch := harness.watchPlayback(allPlayers)
	defer func() { failure = errors.Join(failure, watch.finish(harness)) }()
	retirements := map[string]any{}
	harness.results["passerby_retirements"] = retirements
	idle, err := harness.passerbyRetired("idle-publisher-idle", "idle", "sample", publishers["idle"], 6*time.Second)
	if err != nil {
		return err
	}
	idleElapsed := time.Since(idleObserved).Seconds()
	if idleElapsed < 2.5 || idleElapsed > 6 {
		return fmt.Errorf("initial no-viewer retirement outside idle budget: %.3fs", idleElapsed)
	}
	idle["seconds_since_observed_publishing"] = idleElapsed
	retirements["initial-no-viewers"] = idle
	if err := harness.finishPlayers(map[string]*process{"hls-bootstrap": bootstrap}, 180); err != nil {
		return err
	}
	if err := harness.waitAt(10); err != nil {
		return err
	}
	for index := range harness.settings.Application {
		app := &harness.settings.Application[index]
		switch app.AppName {
		case "keep":
			app.Passerby = true
		case "stateful", "hls":
			app.Passerby = false
		}
	}
	if err := harness.commit("flip-passerby-for-new-publications", nil); err != nil {
		return err
	}
	freshKeep, err := harness.publishInstance("idle-publisher-fresh-keep", "keep", "fresh", 45)
	if err != nil {
		return err
	}
	freshStateful, err := harness.publishInstance("publish-fresh-stateful", "stateful", "fresh", 44)
	if err != nil {
		return err
	}
	if err := harness.until("new publications after flip", 2*time.Second, func() (bool, error) {
		for _, app := range []string{"keep", "stateful"} {
			count, err := harness.publicationCount(app, "fresh", "pub")
			if err != nil || count != 1 {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	freshObserved := time.Now()
	freshIdle, err := harness.passerbyRetired("idle-publisher-fresh-keep", "keep", "fresh", freshKeep, 6*time.Second)
	if err != nil {
		return err
	}
	freshElapsed := time.Since(freshObserved).Seconds()
	if freshElapsed < 2.5 || freshElapsed > 6 {
		return fmt.Errorf("new Passerby publication did not use changed policy: %.3fs", freshElapsed)
	}
	freshIdle["seconds_since_observed_publishing"] = freshElapsed
	retirements["new-true-policy"] = freshIdle
	if err := harness.waitAt(21); err != nil {
		return err
	}
	for _, stream := range []struct{ app, instance string }{{"keep", "sample"}, {"stateful", "fresh"}} {
		count, err := harness.publicationCount(stream.app, stream.instance, "pub")
		if err != nil || count != 1 {
			return fmt.Errorf("false-policy publication retired without viewers: %s/%s count=%d error=%v", stream.app, stream.instance, count, err)
		}
		data, err := harness.adminData(harness.ctx, "/admin/stat/sub/online?key="+url.QueryEscape(stream.app+"/"+stream.instance))
		if err != nil || data["count"] != float64(0) {
			return fmt.Errorf("false-policy proof had external viewers: %s/%s %v %v", stream.app, stream.instance, data, err)
		}
	}
	harness.results["false_policy_retained_without_viewers"] = map[string]any{"old_keep_after_true_reload": true, "new_stateful_after_false_reload": true, "observed_at_seconds": time.Since(harness.started).Seconds()}
	hlsOnline, err := harness.adminData(harness.ctx, "/admin/stat/sub/online?key=hls/sample")
	if err != nil || hlsOnline["count"] != float64(0) || !hlsPlayer.alive() || !publishers["hls"].alive() || observation.segmentRequests.Load() < 3 {
		return fmt.Errorf("HLS-only audience did not retain source: online=%v error=%v", hlsOnline, err)
	}
	harness.results["hls_only_retained_after_idle_window"] = true
	if err := harness.finishPlayers(stateful, 575); err != nil {
		return err
	}
	statefulRetired, err := harness.passerbyRetired("idle-publisher-stateful", "stateful", "sample", publishers["stateful"], 6*time.Second)
	if err != nil {
		return err
	}
	retirements["old-true-policy-after-stateful-viewers"] = statefulRetired
	if err := harness.finishPlayers(map[string]*process{"hls-only": hlsPlayer}, 630); err != nil {
		return err
	}
	expectedDeadline := time.Unix(0, observation.deadline.Load())
	if !publishers["hls"].alive() || observation.deadline.Load() == 0 {
		return errors.New("HLS publication ended before its issued-viewer window")
	}
	hlsRetired, err := harness.passerbyRetired("idle-publisher-hls", "hls", "sample", publishers["hls"], max(time.Second, time.Until(expectedDeadline)+3*time.Second))
	if err != nil {
		return err
	}
	delay := time.Since(expectedDeadline).Seconds()
	if delay < -0.5 || delay > 2.5 {
		return fmt.Errorf("HLS retirement outside issued window: %.3fs", delay)
	}
	hlsRetired["seconds_after_issued_deadline"] = delay
	hlsRetired["playlist_requests"], hlsRetired["segment_requests"] = observation.playlistRequests.Load(), observation.segmentRequests.Load()
	retirements["old-true-policy-after-hls-viewers"] = hlsRetired
	closedRecordings, err := harness.retiredRecordingSizes()
	if err != nil {
		return err
	}
	freshPlayers := map[string]*process{}
	for protocol, address := range map[string]string{
		"rtmp":     fmt.Sprintf("rtmp://127.0.0.1:%d/stateful/fresh", harness.rtmpPort),
		"http-flv": harness.origin + "/hfs/stateful/fresh.flv",
		"hls":      harness.origin + "/hls/stateful/fresh.m3u8",
	} {
		name := "fresh-" + protocol
		child, err := harness.originViewer(name, address, 12)
		if err != nil {
			return err
		}
		freshPlayers[name] = child
	}
	if err := harness.finishPlayers(freshPlayers, 275); err != nil {
		return err
	}
	finalRecordings, err := harness.retiredRecordingSizes()
	if err != nil || !maps.Equal(closedRecordings, finalRecordings) {
		return fmt.Errorf("retired recordings changed during subsequent playback: %v", err)
	}
	harness.results["retired_recordings_stable_before_shutdown"] = map[string]any{"count": len(closedRecordings), "unchanged_across_fresh_playback": true}
	if err := harness.finishPlayers(control, 1100); err != nil {
		return err
	}
	for _, app := range []string{"idle", "stateful", "hls"} {
		if err := harness.waitRelayPlaylistStopped(app); err != nil {
			return err
		}
	}
	for _, app := range []string{"control", "keep"} {
		if err := harness.finish("publish-"+app, publishers[app], 65*time.Second); err != nil {
			return err
		}
	}
	if err := harness.finish("publish-fresh-stateful", freshStateful, 50*time.Second); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, app := range []string{"control", "keep", "idle", "stateful", "hls"} {
		if err := harness.recordings(filepath.Join("recordings", app), 1, true); err != nil {
			return err
		}
	}
	logBody, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	events, warnings, err := validatePasserbyEvents(string(logBody))
	if err != nil {
		return err
	}
	harness.results["passerby_idle_events"], harness.results["preexisting_fragment_warning_count"] = events, warnings
	harness.results["timestamp_warning_boundary"] = "The known Fragment warning is retained; unknown warnings and every ERROR fail. Full packet/source timestamp comparison remains open."
	harness.results["passerby_media_verified"] = true
	return harness.checkLogs("play-*.log", "publish-*.log")
}
