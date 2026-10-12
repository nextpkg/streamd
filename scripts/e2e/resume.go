package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type packetProbe struct {
	mediaProbe
	Packets []struct {
		CodecType string `json:"codec_type"`
		DTS       string `json:"dts_time"`
		Flags     string `json:"flags"`
	} `json:"packets"`
}

func (harness *harness) probePackets(filename string) (packetProbe, error) {
	var probe packetProbe
	ctx, cancel := context.WithTimeout(harness.ctx, 15*time.Second)
	defer cancel()
	for _, arguments := range [][]string{
		{"-v", "error", "-count_frames", "-show_entries", "stream=codec_type,nb_read_frames:format=duration", "-of", "json", filename},
		{"-v", "error", "-fflags", "+nofillin", "-show_packets", "-show_entries", "packet=codec_type,dts_time,flags", "-of", "json", filename},
	} {
		command := exec.CommandContext(ctx, "ffprobe", arguments...)
		var stderr bytes.Buffer
		command.Stderr = &stderr
		body, err := command.Output()
		if err != nil || stderr.Len() != 0 {
			return probe, fmt.Errorf("probe %s: %v: %s", filename, err, stderr.String())
		}
		if err := json.Unmarshal(body, &probe); err != nil {
			return probe, err
		}
	}
	return probe, nil
}

func verifyResumedMedia(probe packetProbe, expectedFrames map[string]int, expectedDuration float64) error {
	if err := probe.validate(expectedDuration + 0.5); err != nil {
		return err
	}
	for _, stream := range probe.Streams {
		frames, err := strconv.Atoi(stream.Frames)
		if err != nil || frames != expectedFrames[stream.CodecType] {
			return fmt.Errorf("%s decoded %d frames, expected exactly %d", stream.CodecType, frames, expectedFrames[stream.CodecType])
		}
	}
	duration, err := strconv.ParseFloat(probe.Format.Duration, 64)
	if err != nil || math.Abs(duration-expectedDuration) > 0.5 {
		return fmt.Errorf("duration %s differs from input duration %.3f", probe.Format.Duration, expectedDuration)
	}
	previous := make(map[string]float64)
	counts := make(map[string]int)
	for _, packet := range probe.Packets {
		timestamp, err := strconv.ParseFloat(packet.DTS, 64)
		if err != nil || math.IsNaN(timestamp) || math.IsInf(timestamp, 0) {
			return fmt.Errorf("invalid %s DTS %q", packet.CodecType, packet.DTS)
		}
		if last, exists := previous[packet.CodecType]; exists && (timestamp <= last || timestamp-last > 0.25) {
			return fmt.Errorf("%s DTS discontinuity: %.6f -> %.6f", packet.CodecType, last, timestamp)
		}
		previous[packet.CodecType] = timestamp
		counts[packet.CodecType]++
	}
	for codec, frames := range expectedFrames {
		if counts[codec] != frames {
			return fmt.Errorf("%s packet count %d does not match decoded frame count %d", codec, counts[codec], frames)
		}
	}
	return nil
}

func (harness *harness) resume() error {
	if remaining := 3600 - time.Now().Unix()%3600; remaining < 60 {
		if err := harness.sleep(time.Duration(remaining+1) * time.Second); err != nil {
			return err
		}
	}
	harness.recordingApplications("round", 3600, 2)
	if harness.object != nil {
		for index := range harness.settings.Application {
			recorder := &harness.settings.Application[index].Recorder
			recorder.Driver = harness.object.provider
			if recorder.Driver == "aws" {
				recorder.AWS = harness.object.config
			} else {
				recorder.COS = harness.object.config
			}
		}
	}
	if err := harness.start(); err != nil {
		return err
	}
	started := harness.started
	expectedFrames, expectedDuration := make(map[string]int), float64(0)
	paths := make(map[string]string)
	harness.results["sessions"] = []map[string]any{}
	harness.results["publish_seconds"] = 15
	for session, fixture := range []string{"regular", "bframes", "regular"} {
		if session == 2 {
			if err := harness.stopServer(); err != nil {
				return err
			}
			harness.results["shutdown_before_restart"] = harness.results["shutdown"]
			if err := os.Rename(filepath.Join(harness.evidence, "server.log"), filepath.Join(harness.evidence, "server-before-restart.log")); err != nil {
				return err
			}
			if err := harness.start(); err != nil {
				return err
			}
			harness.started = started
		}
		source := filepath.Join(harness.evidence, fmt.Sprintf("session-%d.flv", session))
		arguments := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-i", filepath.Join(harness.root, "testdata", "generated", fixture+".flv"), "-t", "5"}
		if harness.object == nil {
			arguments = append(arguments, "-c", "copy")
		} else {
			bframes := "0"
			if fixture == "bframes" {
				bframes = "2"
			}
			arguments = append(arguments, "-vf", "scale=1280:720,noise=alls=24:allf=t+u:all_seed=1", "-c:a", "copy", "-c:v", "libx264", "-preset", "ultrafast", "-g", "25", "-bf", bframes,
				"-b:v", "12M", "-minrate", "12M", "-maxrate", "12M", "-bufsize", "12M", "-x264-params", "nal-hrd=cbr:force-cfr=1")
		}
		arguments = append(arguments, source)
		generate, err := harness.launch(fmt.Sprintf("fixture-%d", session), append([]string{"ffmpeg"}, arguments...)...)
		if err != nil {
			return err
		}
		if err := harness.finish(fmt.Sprintf("fixture-%d", session), generate, 15*time.Second); err != nil {
			return err
		}
		input, err := harness.probePackets(source)
		if err != nil {
			return err
		}
		if err := input.validate(5.5); err != nil {
			return err
		}
		boundarySeen := false
		preroll := make(map[string]int)
		for _, packet := range input.Packets {
			if packet.CodecType == "video" && strings.Contains(packet.Flags, "K") {
				boundarySeen = true
			}
			if boundarySeen {
				expectedFrames[packet.CodecType]++
			} else {
				preroll[packet.CodecType]++
			}
		}
		if !boundarySeen {
			return errors.New("reference input contains no video keyframe")
		}
		duration, err := strconv.ParseFloat(input.Format.Duration, 64)
		if err != nil {
			return err
		}
		expectedDuration += duration
		publishers := make(map[string]*process)
		for _, app := range []string{"flv", "ts"} {
			child, err := harness.launch(fmt.Sprintf("publish-%d-%s", session, app), "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror",
				"-re", "-i", source, "-c", "copy", "-f", "flv", fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app))
			if err != nil {
				return err
			}
			publishers[app] = child
		}
		for app, child := range publishers {
			if err := harness.finish(fmt.Sprintf("publish-%d-%s", session, app), child, 15*time.Second); err != nil {
				return err
			}
		}
		if err := harness.until("recorders finalized after disconnect", 10*time.Second, func() (bool, error) {
			report, err := harness.configuration()
			for _, component := range report.Components {
				if component.Component == "recording" {
					return false, err
				}
			}
			return true, err
		}); err != nil {
			return err
		}
		checks := make(map[string]any)
		for _, app := range []string{"flv", "ts"} {
			if harness.object != nil {
				state, err := harness.objectSnapshot(app, session == 2)
				if err != nil {
					return err
				}
				checks[app] = state
				if session != 2 {
					continue
				}
			}
			files, err := recordingFiles(filepath.Join(harness.evidence, "recordings", app))
			if err != nil {
				return err
			}
			if len(files) != 1 || (paths[app] != "" && paths[app] != files[0]) {
				return errors.New("round resume must retain exactly the same filename")
			}
			paths[app] = files[0]
			probe, err := harness.probePackets(files[0])
			if err != nil {
				return err
			}
			if harness.object == nil {
				checks[app] = probe.mediaProbe
			} else {
				checks[app].(map[string]any)["media"] = probe.mediaProbe
			}
			if err := verifyResumedMedia(probe, expectedFrames, expectedDuration); err != nil {
				return fmt.Errorf("session %d %s: %w", session, app, err)
			}
			if err := harness.recordings(filepath.Join("recordings", app), 1, false); err != nil {
				return err
			}
		}
		harness.results["sessions"] = append(harness.results["sessions"].([]map[string]any), map[string]any{
			"fixture": fixture, "restart_before_session": session == 2, "expected_frames": map[string]int{"video": expectedFrames["video"], "audio": expectedFrames["audio"]},
			"expected_duration": expectedDuration, "recordings": checks, "same_filename": true, "monotonic_dts": harness.object == nil || session == 2, "input_before_first_keyframe": preroll})
		if err := harness.sleep(time.Second); err != nil {
			return err
		}
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	return harness.checkLogs("play-*.log", "publish-*.log", "fixture-*.log")
}
