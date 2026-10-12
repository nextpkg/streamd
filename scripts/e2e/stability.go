package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nextpkg/streamd/config"
)

func stabilityDuration() (int, error) {
	seconds := 60
	if value := os.Getenv("STREAMD_E2E_STABILITY_SECONDS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 60 || parsed > 7200 {
			return 0, errors.New("STREAMD_E2E_STABILITY_SECONDS must be an integer from 60 through 7200")
		}
		seconds = parsed
	}
	return seconds, nil
}

func (harness *harness) stabilityPlayers(app, prefix string, seconds int) (map[string]*process, error) {
	sources := map[string]string{"rtmp": fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app), "http-flv": harness.origin + "/hfs/" + app + "/sample.flv", "hls": harness.origin + "/hls/" + app + "/sample.m3u8"}
	players := make(map[string]*process)
	for protocol, source := range sources {
		name := prefix + protocol
		arguments := []string{"ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000"}
		if protocol == "hls" {
			arguments = append(arguments, "-http_multiple", "0")
		}
		arguments = append(arguments, "-i", source, "-t", strconv.Itoa(seconds), "-progress", filepath.Join(harness.evidence, "progress-"+name+".txt"), "-f", "null", "-")
		child, err := harness.launch("play-"+name, arguments...)
		if err != nil {
			return nil, err
		}
		players[name] = child
	}
	return players, nil
}

func (harness *harness) stabilityMedia() (result error) {
	seconds, err := stabilityDuration()
	if err != nil {
		return err
	}
	expectedRecordings, err := harness.stabilityRecordingPlan(seconds + 40)
	if err != nil {
		return err
	}
	harness.recordingApplications("immediate", 4, 3)
	harness.settings.Metrics = &config.Metrics{Cluster: "stability-0"}
	if err := harness.start(); err != nil {
		return err
	}
	if err := harness.release.verifyCLI(harness); err != nil {
		return err
	}
	monitor, err := harness.startStabilityMonitor()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, monitor.close(harness)) }()
	initial, err := harness.resourceSnapshot()
	if err != nil {
		return err
	}
	harness.results["resource_initial"] = initial
	harness.results["stability_hls_client"] = map[string]any{"http_multiple": false, "http_persistent": true, "reason": "avoid FFmpeg retaining an unused prefetch connection beyond the server idle timeout"}
	harness.results["stability_conditions"] = map[string]any{"continuous_seconds": seconds, "publish_seconds": seconds + 40, "play_seconds": seconds + 15, "publishers": 2, "players": 6, "container_cpus": 1, "container_memory_bytes": 512 * 1024 * 1024, "resource_sampling_seconds": 1, "maximum_media_gap_seconds": 5, "reload_count": 10, "fixture": "bframes.flv", "progress_reader": "bounded incremental reads", "force_gc": false}
	publishers, err := harness.publishApps("publish-", "bframes", seconds+40)
	if err != nil {
		return err
	}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
		group, err := harness.stabilityPlayers(app, app+"-", seconds+15)
		if err != nil {
			return err
		}
		maps.Copy(players, group)
	}
	progress := map[string]*stabilityProgress{}
	advancement := map[string]*frameProgress{}
	for name := range players {
		progress[name], advancement[name] = &stabilityProgress{}, &frameProgress{}
	}
	trace, err := os.OpenFile(filepath.Join(harness.evidence, "stability-trace.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, trace.Close()) }()
	encoder := json.NewEncoder(trace)
	launched, warmed := time.Now(), time.Time{}
	samples := []resourceSample{}
	reloads := 0
	cluster := "stability-0"
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
observation:
	for {
		select {
		case <-harness.ctx.Done():
			return harness.ctx.Err()
		case now := <-ticker.C:
			if !harness.server.alive() {
				return errors.New("stability service exited during playback")
			}
			for app, child := range publishers {
				if err := requireSilentMediaLog(filepath.Join(harness.evidence, "publish-"+app+".log")); err != nil {
					return err
				}
				if !child.alive() {
					return fmt.Errorf("publisher %s exited during the observation window", app)
				}
			}
			ready := true
			frames := map[string]int{}
			for name, child := range players {
				if err := requireSilentMediaLog(filepath.Join(harness.evidence, "play-"+name+".log")); err != nil {
					return err
				}
				if !child.alive() {
					return fmt.Errorf("player %s exited during the observation window", name)
				}
				count, err := progress[name].read(filepath.Join(harness.evidence, "progress-"+name+".txt"))
				if err != nil {
					return err
				}
				if err := advancement[name].observe(now, count, 5*time.Second); err != nil {
					return fmt.Errorf("%s: %w", name, err)
				}
				frames[name] = count
				ready = ready && count > 0
			}
			if warmed.IsZero() && ready {
				warmed = now
				harness.results["stability_warmup_seconds"] = now.Sub(launched).Seconds()
			}
			if warmed.IsZero() && now.Sub(launched) > 12*time.Second {
				return errors.New("all six players did not decode within twelve seconds")
			}
			sample, err := harness.resourceSnapshot()
			if err != nil {
				return err
			}
			if sample.Values["process_resident_memory_bytes"] > 384*1024*1024 {
				return errors.New("service RSS exceeded the declared 384 MiB stability bound")
			}
			if !warmed.IsZero() {
				sample.Seconds = now.Sub(warmed).Seconds()
				playlist, err := harness.playlist("flv")
				if err != nil {
					return err
				}
				location, err := segmentURL(harness.origin, "flv", playlist)
				if err != nil {
					return err
				}
				started := time.Now()
				status, body, err := harness.fetch(location)
				sample.HTTPSeconds, sample.HTTPBodyBytes = time.Since(started).Seconds(), len(body)
				if err != nil || status != http.StatusOK || len(body) == 0 || len(body)%188 != 0 {
					return fmt.Errorf("stability HLS probe HTTP %d: %v", status, err)
				}
				samples = append(samples, sample)
			}
			if err := encoder.Encode(map[string]any{"since_launch_seconds": now.Sub(launched).Seconds(), "resource": sample, "frames": frames}); err != nil {
				return err
			}
			if len(samples)%30 == 0 {
				if err := trace.Sync(); err != nil {
					return err
				}
			}
			if warmed.IsZero() {
				continue
			}
			elapsed := now.Sub(warmed)
			if reloads < 10 && elapsed >= time.Duration(seconds*(reloads+1)/11)*time.Second {
				reloads++
				cluster = fmt.Sprintf("stability-%d", reloads%2)
				harness.settings.Metrics.Cluster = cluster
				expected := map[componentKey]string{}
				for index := range harness.settings.Application {
					application := &harness.settings.Application[index]
					application.HLS.PlaylistLength = 3 + reloads%2
					expected[componentKey{"hls", application.AppName + "/sample"}] = "active"
					expected[componentKey{"recording", application.AppName + "/sample"}] = "active"
				}
				if err := harness.commit(fmt.Sprintf("distributed-reload-%02d", reloads), expected); err != nil {
					return err
				}
			}
			if elapsed >= time.Duration(seconds)*time.Second {
				harness.results["stability_observed_seconds"] = elapsed.Seconds()
				break observation
			}
		}
	}
	if err := trace.Sync(); err != nil {
		return err
	}
	summary, err := summarizeStabilityResources(samples, seconds)
	if err != nil {
		return err
	}
	harness.results["stability_resources"] = summary
	harness.results["stability_playback"] = advancement
	if reloads != 10 {
		return fmt.Errorf("only %d distributed updates completed", reloads)
	}
	if err := monitor.query(harness); err != nil {
		return err
	}
	if err := harness.finishPlayers(players, (seconds+15)*25*99/100); err != nil {
		return err
	}
	for name := range players {
		if _, err := progress[name].read(filepath.Join(harness.evidence, "progress-"+name+".txt")); err != nil || !progress[name].ended {
			return fmt.Errorf("player %s lacks a complete progress end: %v", name, err)
		}
	}
	for app, child := range publishers {
		if err := harness.finish("publish-"+app, child, 45*time.Second); err != nil {
			return err
		}
	}
	if err := harness.metricSnapshot("stability-retired", cluster, false); err != nil {
		return err
	}
	var retired resourceSample
	if err := harness.until("stability media components and resources retire", 15*time.Second, func() (bool, error) {
		var err error
		retired, err = harness.resourceSnapshot()
		if err != nil {
			return false, err
		}
		report, err := harness.configuration()
		observations, _ := harness.results["stability_retirement"].([]map[string]any)
		harness.results["stability_retirement"] = append(observations, map[string]any{"resource": retired, "components": report.Components})
		return err == nil && len(report.Components) == 0 && retired.Values["go_goroutines"] <= initial.Values["go_goroutines"]+4 && retired.Values["process_open_fds"] <= initial.Values["process_open_fds"]+2, err
	}); err != nil {
		return err
	}
	harness.results["resource_retired"] = retired
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		previous, _ := harness.results["recordings"].([]recordingResult)
		if err := harness.recordings(filepath.Join("recordings", app), len(expectedRecordings), true); err != nil {
			return err
		}
		verified := harness.results["recordings"].([]recordingResult)
		if err := verifyStabilityRecordingFrames(verified[len(previous):], expectedRecordings); err != nil {
			return fmt.Errorf("%s: %w", app, err)
		}
	}
	harness.results["stability_recording_frames_verified"] = true
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var event struct{ Level string }
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return err
		}
		if event.Level == "WARN" || event.Level == "ERROR" {
			return fmt.Errorf("unexpected stability service event: %s", line)
		}
	}
	harness.results["stability_verified"] = true
	return harness.checkLogs("publish-*.log", "play-*.log")
}
