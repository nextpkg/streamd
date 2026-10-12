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
)

func verifyContinuousMedia(files []recordingIdentity, source recordingIdentity, segmentUS, audioTolerance int64, resetClock bool) ([]recordingIdentityResult, error) {
	if len(files) == 0 || len(source.Video) == 0 || len(source.Audio) == 0 || len(source.Video) != len(source.Tracks["video"]) || len(source.Audio) != len(source.Tracks["audio"]) {
		return nil, errors.New("missing continuous media identity")
	}
	positions := map[string]int{"video": 0, "audio": 0}
	for resetClock && positions["audio"] < len(source.Audio) && source.Tracks["audio"][positions["audio"]].DTS < source.Tracks["video"][0].DTS {
		positions["audio"]++
	}
	var results []recordingIdentityResult
	var continuousOffset int64
	for fileIndex, file := range files {
		video := file.Tracks["video"]
		if len(video) == 0 || len(video) != len(file.Video) || len(file.Audio) == 0 || len(file.Audio) != len(file.Tracks["audio"]) || !video[0].Key || positions["video"] >= len(source.Video) {
			return nil, fmt.Errorf("%s: invalid first keyframe or packet counts", file.File)
		}
		offset := video[0].DTS - source.Tracks["video"][positions["video"]].DTS
		if resetClock && video[0].DTS != 0 {
			return nil, errors.New("recording did not reset its clock at the keyframe")
		}
		if !resetClock && fileIndex > 0 && offset != continuousOffset {
			return nil, errors.New("HLS timeline offset changed between segments")
		}
		continuousOffset = offset
		result := recordingIdentityResult{File: file.File, SHA256: file.SHA256, VideoFrames: len(file.Video), AudioPackets: len(file.Audio), Duration: file.Media.Format.Duration}
		for _, track := range []string{"video", "audio"} {
			actual, expected, tolerance := file.Video, source.Video, int64(1)
			if track == "audio" {
				actual, expected, tolerance = file.Audio, source.Audio, audioTolerance
			}
			if positions[track]+len(actual) > len(expected) {
				return nil, fmt.Errorf("%s has extra %s packets", file.File, track)
			}
			for index, digest := range actual {
				position := positions[track] + index
				packet, reference := file.Tracks[track][index], source.Tracks[track][position]
				if digest != expected[position] {
					return nil, fmt.Errorf("%s %s payload mismatch at source packet %d", file.File, track, position)
				}
				if absolute(packet.DTS-reference.DTS-offset) > tolerance || absolute(packet.PTS-reference.PTS-offset) > tolerance || packet.Key != reference.Key {
					return nil, fmt.Errorf("%s %s timestamp/keyframe mismatch at source packet %d", file.File, track, position)
				}
				if index > 0 && packet.DTS <= file.Tracks[track][index-1].DTS {
					return nil, fmt.Errorf("%s non-increasing %s timestamp", file.File, track)
				}
				if track == "video" && index > 0 && packet.Key && packet.DTS-video[0].DTS >= segmentUS {
					return nil, errors.New("continuous media skipped an eligible cutting keyframe")
				}
				if track == "audio" {
					result.MaximumAudioErrorUS = max(result.MaximumAudioErrorUS, absolute(packet.DTS-reference.DTS-offset), absolute(packet.PTS-reference.PTS-offset))
				}
			}
			positions[track] += len(actual)
		}
		if fileIndex+1 < len(files) {
			if positions["video"] >= len(source.Video) {
				return nil, errors.New("extra segment after the source ended")
			}
			next := source.Tracks["video"][positions["video"]]
			boundary := next.DTS + offset - video[0].DTS
			if !next.Key || boundary < segmentUS {
				return nil, errors.New("continuous media cut before its target")
			}
			result.NextKeyframeSeconds = float64(boundary) / 1e6
		}
		if err := file.Media.validate(float64(segmentUS)/1e6 + 0.5); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if positions["video"] != len(source.Video) || positions["audio"] != len(source.Audio) {
		return nil, fmt.Errorf("continuous media omitted source tail: video=%d/%d audio=%d/%d", positions["video"], len(source.Video), positions["audio"], len(source.Audio))
	}
	return results, nil
}

func (harness *harness) captureClockPlaylist(app string, seen map[string]bool) (bool, error) {
	playlist, err := harness.playlist(app)
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(playlist, "\n") {
		if line == "" || strings.HasPrefix(line, "#") || seen[line] {
			continue
		}
		status, body, err := harness.fetch("/hls/" + app + "/" + line)
		if err != nil || status != http.StatusOK {
			return false, fmt.Errorf("capture HLS %s: HTTP %d: %v", line, status, err)
		}
		filename := filepath.Join(harness.evidence, "hls-capture", app, fmt.Sprintf("%05d.ts", len(seen)))
		if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
			return false, err
		}
		if err := os.WriteFile(filename, body, 0600); err != nil {
			return false, err
		}
		seen[line] = true
	}
	return strings.Contains(playlist, "#EXT-X-ENDLIST"), nil
}

func (harness *harness) fragmentClock() error {
	fixture := os.Getenv("STREAMD_E2E_CLOCK_FIXTURE")
	if fixture == "" {
		fixture = "regular"
	}
	if fixture != "regular" && fixture != "bframes" {
		return errors.New("clock fixture must be regular or bframes")
	}
	const seconds = 60
	referenceFile := filepath.Join(harness.evidence, "source-loop.flv")
	if _, err := mediaCommand(harness.ctx, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-y", "-stream_loop", "-1", "-i", filepath.Join(harness.root, "testdata", "generated", fixture+".flv"), "-t", strconv.Itoa(seconds), "-c", "copy", "-f", "flv", referenceFile); err != nil {
		return err
	}
	source, err := inspectRecording(harness.ctx, referenceFile)
	if err != nil {
		return err
	}
	harness.results["source_identity"] = source
	harness.results["fixture"] = fixture
	harness.recordingApplications("immediate", 4, 3)
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", fixture, seconds)
	if err != nil {
		return err
	}
	captures, err := harness.startLiveIdentityCaptures()
	if err != nil {
		return err
	}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
		group, err := harness.players(app, app+"-", 50)
		if err != nil {
			return err
		}
		for name, child := range group {
			players[name] = child
		}
	}
	watch := harness.watchPlayback(players)
	defer watch.finish(harness)
	seen := map[string]map[string]bool{"flv": {}, "ts": {}}
	ended := map[string]bool{}
	if err := harness.until("all HLS tails captured", 70*time.Second, func() (bool, error) {
		for _, app := range []string{"flv", "ts"} {
			if ended[app] {
				continue
			}
			complete, err := harness.captureClockPlaylist(app, seen[app])
			if err != nil {
				return false, err
			}
			ended[app] = complete
		}
		return ended["flv"] && ended["ts"], nil
	}); err != nil {
		return err
	}
	if err := harness.finishPlayers(players, 50*25*9/10); err != nil {
		return err
	}
	if err := watch.finish(harness); err != nil {
		return err
	}
	for app, child := range publishers {
		if err := harness.finish("publish-"+app, child, 5*time.Second); err != nil {
			return err
		}
	}
	if err := harness.finishLiveIdentityCaptures(captures, source); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, group := range []struct {
		directory string
		segment   int64
		reset     bool
		tolerance int64
	}{
		{"recordings/flv", 4000000, true, 1}, {"recordings/ts", 4000000, true, 10001},
		{"hls-capture/flv", 2000000, false, 10001}, {"hls-capture/ts", 2000000, false, 10001},
	} {
		if err := harness.recordingsWithDuration(group.directory, 10, true, float64(group.segment)/1e6+0.5); err != nil {
			return err
		}
		paths, err := recordingFiles(filepath.Join(harness.evidence, group.directory))
		if err != nil {
			return err
		}
		var files []recordingIdentity
		for _, filename := range paths {
			identity, err := inspectRecording(harness.ctx, filename)
			if err != nil {
				return err
			}
			files = append(files, identity)
		}
		verified, err := verifyContinuousMedia(files, source, group.segment, group.tolerance, group.reset)
		if err != nil {
			return err
		}
		harness.results["identity_"+group.directory] = verified
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" {
			continue
		}
		var event struct{ Level, Msg string }
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return err
		}
		if event.Level == "WARN" || event.Level == "ERROR" {
			return fmt.Errorf("unexpected service event: %s", line)
		}
	}
	harness.results["continuous_media_identity_verified"] = true
	return harness.checkLogs("publish-*.log", "play-*.log")
}
