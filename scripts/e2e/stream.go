package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (harness *harness) recordingApplications(mode string, segment, window int) {
	for _, kind := range []string{"flv", "ts"} {
		harness.settings.Application = append(harness.settings.Application, appConfig{
			Enable: true, AppName: kind, HLS: hlsConfig{Enable: true, Fragment: 2, PlaylistLength: window},
			Recorder: recorderConfig{Enable: true, SaveAs: kind, SaveMode: mode, Segment: segment,
				Driver: "fs", SaveTo: filepath.Join(harness.evidence, "recordings")},
		})
	}
}

func (harness *harness) publishApps(prefix, fixture string, seconds int) (map[string]*process, error) {
	publishers := make(map[string]*process)
	for _, app := range harness.settings.Application {
		child, err := harness.publish(prefix+app.AppName, app.AppName, fixture, seconds)
		if err != nil {
			return nil, err
		}
		publishers[app.AppName] = child
	}
	return publishers, nil
}

func (harness *harness) readiness() ([]byte, error) {
	status, body, err := harness.fetch("/readyz")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("readiness HTTP %d", status)
	}
	return body, nil
}

func (harness *harness) stream(duration int) error {
	harness.results["publish_seconds"] = duration
	harness.recordingApplications("immediate", 4, 2)
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", "regular", duration)
	if err != nil {
		return err
	}
	playlist, err := harness.waitPlaylist("flv")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(harness.evidence, "playlist.m3u8"), []byte(playlist), 0600); err != nil {
		return err
	}
	playSeconds := min(40, duration-12)
	players, err := harness.players("flv", "", playSeconds)
	if err != nil {
		return err
	}
	if err := harness.sleep(2 * time.Second); err != nil {
		return err
	}
	for revision := 2; revision <= 101; revision++ {
		harness.settings.LogLevel = "WARN"
		if revision%2 == 1 {
			harness.settings.LogLevel = "INFO"
		}
		if revision == 2 {
			harness.settings.LogLevel = "DEBUG"
			harness.settings.RTMP.ChunkSize = 4096
		}
		if err := harness.atomicSettings(); err != nil {
			return err
		}
		if err := harness.until(fmt.Sprintf("configuration %d", revision), 12*time.Second, func() (bool, error) {
			report, err := harness.configuration()
			return report.Revision == revision, err
		}); err != nil {
			return err
		}
		ready, err := harness.readiness()
		if err != nil || !strings.Contains(string(ready), fmt.Sprintf("configuration=%d", revision)) {
			return fmt.Errorf("configuration %d not reflected in readiness: %s: %v", revision, ready, err)
		}
		if revision == 2 {
			harness.results["reload"] = strings.TrimSpace(string(ready))
		}
	}
	harness.results["reload_commits"] = 100
	previous, err := harness.readiness()
	if err != nil {
		return err
	}
	if err := harness.atomicConfiguration([]byte("rtmp:\n  chunk_size: 1\n")); err != nil {
		return err
	}
	if err := harness.sleep(300 * time.Millisecond); err != nil {
		return err
	}
	current, err := harness.readiness()
	if err != nil {
		return err
	}
	if !bytes.Equal(previous, current) {
		return fmt.Errorf("invalid candidate changed readiness from %q to %q", previous, current)
	}
	harness.results["invalid_reload_retained_active"] = true
	if err := harness.finishPlayers(players, playSeconds*25*9/10); err != nil {
		return err
	}
	if err := harness.finishPublishers(publishers, "publish-", time.Duration(duration+10)*time.Second); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		if err := harness.recordings(filepath.Join("recordings", app), 1, false); err != nil {
			return err
		}
	}
	return harness.checkLogs("play-*.log", "publish-*.log")
}

type subscriberReport struct {
	Count int `json:"count"`
	List  []struct {
		PTS           float64 `json:"pts"`
		Duration      float64 `json:"duration"`
		VideoReceived float64 `json:"videoReceived"`
	} `json:"list"`
}

func (harness *harness) subscribers() (map[string]subscriberReport, error) {
	reports := make(map[string]subscriberReport)
	for _, app := range []string{"flv", "ts"} {
		status, body, err := harness.fetch("/admin/stat/sub/info?key=" + app + "/sample")
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("subscribers for %s returned HTTP %d", app, status)
		}
		var envelope struct {
			Data subscriberReport `json:"data"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			return nil, err
		}
		reports[app] = envelope.Data
	}
	return reports, nil
}

func verifyContinuity(before, after subscriberReport) error {
	if before.Count != 3 || after.Count != 3 || len(before.List) != 3 || len(after.List) != 3 {
		return fmt.Errorf("subscriber count changed or incomplete: before=%+v after=%+v", before, after)
	}
	maximumPTS := before.List[0].PTS
	minimumDuration, minimumBytes := before.List[0].Duration, before.List[0].VideoReceived
	for _, item := range before.List {
		maximumPTS = max(maximumPTS, item.PTS)
		minimumDuration, minimumBytes = min(minimumDuration, item.Duration), min(minimumBytes, item.VideoReceived)
	}
	for _, item := range after.List {
		if item.PTS <= maximumPTS || item.Duration < minimumDuration || item.VideoReceived < minimumBytes {
			return fmt.Errorf("subscriber progress reset: before=%+v after=%+v", before, after)
		}
	}
	return nil
}

func (harness *harness) republish() error {
	return harness.republishWithCallbacks(nil, "")
}

func (harness *harness) republishWithCallbacks(callbacks *combinationReceiver, endpoint string) error {
	harness.recordingApplications("round", 10, 3)
	if callbacks != nil {
		harness.configureTakeoverCallbacks(endpoint, 0)
	}
	harness.results["expected_disconnections"] = []map[string]any{}
	harness.results["takeovers"] = []map[string]any{}
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-0-", "regular", 70)
	if err != nil {
		return err
	}
	if _, err := harness.waitPlaylist("flv"); err != nil {
		return err
	}
	players := make(map[string]*process)
	for _, app := range []string{"flv", "ts"} {
		group, err := harness.players(app, app+"-", 48)
		if err != nil {
			return err
		}
		for name, child := range group {
			players[name] = child
		}
	}
	for generation, fixture := range []string{"bframes", "regular"} {
		generation++
		if err := harness.waitAt(generation * 20); err != nil {
			return err
		}
		if callbacks != nil {
			harness.configureTakeoverCallbacks(endpoint, generation)
			if err := harness.commit(fmt.Sprintf("callback-snapshot-for-generation-%d", generation), nil); err != nil {
				return err
			}
		}
		before, err := harness.subscribers()
		if err != nil {
			return err
		}
		previous := publishers
		publishers, err = harness.publishApps(fmt.Sprintf("publish-%d-", generation), fixture, 60-generation*20)
		if err != nil {
			return err
		}
		for app, child := range previous {
			code, err := child.wait(harness.ctx, 8*time.Second)
			if err != nil {
				return err
			}
			if code == 0 {
				return fmt.Errorf("replaced publisher %s generation %d was not disconnected", app, generation-1)
			}
			harness.results["expected_disconnections"] = append(harness.results["expected_disconnections"].([]map[string]any),
				map[string]any{"generation": generation - 1, "app": app, "returncode": code})
		}
		if err := harness.sleep(2 * time.Second); err != nil {
			return err
		}
		after, err := harness.subscribers()
		if err != nil {
			return err
		}
		for name, child := range players {
			if !child.alive() {
				return fmt.Errorf("player %s exited during takeover", name)
			}
		}
		for app, snapshot := range before {
			if err := verifyContinuity(snapshot, after[app]); err != nil {
				return fmt.Errorf("%s: %w", app, err)
			}
		}
		harness.results["takeovers"] = append(harness.results["takeovers"].([]map[string]any),
			map[string]any{"generation": generation, "fixture": fixture, "before": before, "after": after})
	}
	if err := harness.finishPlayers(players, 1100); err != nil {
		return err
	}
	if err := harness.finishPublishers(publishers, "publish-final-", 30*time.Second); err != nil {
		return err
	}
	if callbacks != nil {
		if err := harness.ordinaryCallbackPublications(callbacks, endpoint); err != nil {
			return err
		}
		if err := harness.until("exact takeover callback generations and recording pairs", 2*time.Second, func() (bool, error) {
			observations, state := callbacks.snapshot()
			if state["invalid"] != "" {
				return false, errors.New("invalid takeover callback")
			}
			directory := filepath.Join(harness.evidence, "recordings")
			files, err := recordingFiles(directory)
			if err != nil {
				return false, err
			}
			counts, err := validateTakeoverCallbacks(observations, files, directory)
			if err == nil {
				harness.results["callback_takeover_counts"] = counts
			}
			return err == nil, nil
		}); err != nil {
			return err
		}
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		if err := harness.recordings(filepath.Join("recordings", app), 5, false); err != nil {
			return err
		}
	}
	harness.results["connection_and_timestamp_continuity"] = true
	if callbacks != nil {
		if err := harness.cloudServerErrors(); err != nil {
			return err
		}
		harness.results["callback_takeover_verified"] = true
	}
	return harness.checkLogs("play-*.log", "publish-2-*.log")
}
