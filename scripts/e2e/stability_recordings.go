package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type stabilityVideoPacket struct {
	DTS   *int64 `json:"dts"`
	Flags string `json:"flags"`
}

func stabilityRecordingFrames(packets []stabilityVideoPacket) ([]int, error) {
	if len(packets) == 0 {
		return nil, errors.New("missing source video packets")
	}
	var frames []int
	var start, previous int64
	for index, packet := range packets {
		if packet.DTS == nil || (index > 0 && *packet.DTS <= previous) {
			return nil, errors.New("missing or non-increasing source DTS")
		}
		key := strings.Contains(packet.Flags, "K")
		if index == 0 {
			if !key {
				return nil, errors.New("source must begin with a keyframe")
			}
			start = *packet.DTS
			frames = append(frames, 0)
		} else if key && *packet.DTS-start >= 4000 {
			start = *packet.DTS
			frames = append(frames, 0)
		}
		frames[len(frames)-1]++
		previous = *packet.DTS
	}
	return frames, nil
}

func verifyStabilityRecordingFrames(recordings []recordingResult, expected []int) error {
	if len(expected) == 0 || len(recordings) != len(expected) {
		return fmt.Errorf("recording count %d differs from source keyframe plan %d", len(recordings), len(expected))
	}
	seen := make(map[string]bool)
	for index, recording := range recordings {
		if recording.File == "" || seen[recording.File] || recording.Media == nil || recording.ReturnCode != 0 || recording.Stderr != "" {
			return errors.New("missing, duplicate or unsuccessful recording")
		}
		seen[recording.File] = true
		if err := recording.Media.validate(4.5); err != nil {
			return err
		}
		videoTracks := 0
		for _, stream := range recording.Media.Streams {
			if stream.CodecType != "video" {
				continue
			}
			videoTracks++
			frames, err := strconv.Atoi(stream.Frames)
			if err != nil || frames != expected[index] || expected[index] <= 0 {
				return fmt.Errorf("%s has %s video frames; source segment requires %d", recording.File, stream.Frames, expected[index])
			}
		}
		if videoTracks != 1 {
			return errors.New("recording must have exactly one video track")
		}
	}
	return nil
}

func (harness *harness) stabilityRecordingPlan(seconds int) ([]int, error) {
	filename := filepath.Join(harness.evidence, "source-loop.flv")
	if _, err := mediaCommand(harness.ctx, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-n", "-stream_loop", "-1", "-i", filepath.Join(harness.root, "testdata", "generated", "bframes.flv"), "-t", strconv.Itoa(seconds), "-c", "copy", "-f", "flv", filename); err != nil {
		return nil, err
	}
	body, err := mediaCommand(harness.ctx, "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_packets", "-show_streams", "-show_entries", "packet=dts,flags:stream=time_base", "-of", "json", filename)
	if err != nil {
		return nil, err
	}
	var source struct {
		Packets []stabilityVideoPacket `json:"packets"`
		Streams []struct {
			TimeBase string `json:"time_base"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(body, &source); err != nil {
		return nil, err
	}
	if len(source.Streams) != 1 || source.Streams[0].TimeBase != "1/1000" {
		return nil, errors.New("source FLV clock must use milliseconds")
	}
	frames, err := stabilityRecordingFrames(source.Packets)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	harness.results["stability_recording_plan"] = map[string]any{"source": "source-loop.flv", "source_sha256": fmt.Sprintf("%x", sha256.Sum256(data)), "video_frames": len(source.Packets), "segments_per_app": len(frames), "segment_video_frames": frames, "boundary": "first source keyframe at least 4000 milliseconds after segment start"}
	return frames, nil
}
