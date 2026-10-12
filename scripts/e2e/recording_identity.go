package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

type recordingPacket struct {
	DTS int64
	PTS int64
	Key bool
}

type recordingIdentity struct {
	File   string
	SHA256 string
	Media  mediaProbe
	Video  []string
	Audio  []string
	Tracks map[string][]recordingPacket
}

type recordingIdentityResult struct {
	File                string  `json:"file"`
	SHA256              string  `json:"sha256"`
	VideoFrames         int     `json:"video_frames"`
	AudioPackets        int     `json:"audio_packets"`
	Duration            string  `json:"container_duration_seconds"`
	NextKeyframeSeconds float64 `json:"next_keyframe_seconds,omitempty"`
	SourceTransition    bool    `json:"source_transition"`
	MaximumAudioErrorUS int64   `json:"maximum_audio_clock_error_us"`
}

func mediaCommand(ctx context.Context, program string, arguments ...string) ([]byte, error) {
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(bounded, program, arguments...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	body, err := command.Output()
	if err != nil || stderr.Len() != 0 {
		return nil, fmt.Errorf("%s: %v: %s", program, err, stderr.String())
	}
	return body, nil
}

func frameDigests(body []byte) ([]string, error) {
	var hashes []string
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) < 6 || (len(fields) > 6 && !strings.HasPrefix(strings.TrimSpace(fields[6]), "S=")) {
			return nil, errors.New("invalid framemd5 row")
		}
		digest := strings.TrimSpace(fields[5])
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != 16 {
			return nil, errors.New("invalid frame digest")
		}
		hashes = append(hashes, digest)
	}
	if len(hashes) == 0 {
		return nil, errors.New("empty frame digest sequence")
	}
	return hashes, nil
}

func timestampUS(value string) (int64, error) {
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || math.Abs(seconds) > 86400 {
		return 0, fmt.Errorf("invalid fixture timestamp %q", value)
	}
	return int64(math.Round(seconds * 1e6)), nil
}

func inspectRecording(ctx context.Context, filename string) (recordingIdentity, error) {
	result := recordingIdentity{File: filename, Tracks: map[string][]recordingPacket{}}
	data, err := os.ReadFile(filename)
	if err != nil {
		return result, err
	}
	result.SHA256 = fmt.Sprintf("%x", sha256.Sum256(data))
	for _, track := range []string{"video", "audio"} {
		arguments := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-i", filename}
		if track == "video" {
			arguments = append(arguments, "-map", "0:v:0", "-fps_mode", "passthrough")
		} else {
			arguments = append(arguments, "-map", "0:a:0", "-c:a", "copy", "-bsf:a", "aac_adtstoasc")
		}
		body, err := mediaCommand(ctx, "ffmpeg", append(arguments, "-f", "framemd5", "-")...)
		if err != nil {
			return result, err
		}
		hashes, err := frameDigests(body)
		if err != nil {
			return result, err
		}
		if track == "video" {
			result.Video = hashes
		} else {
			result.Audio = hashes
		}
	}
	body, err := mediaCommand(ctx, "ffprobe", "-v", "error", "-fflags", "+nofillin", "-count_frames", "-show_packets", "-show_entries", "packet=codec_type,dts_time,pts_time,flags:stream=codec_type,nb_read_frames,sample_rate,r_frame_rate:format=duration", "-of", "json", filename)
	if err != nil {
		return result, err
	}
	var probe struct {
		Packets []struct {
			Type  string `json:"codec_type"`
			DTS   string `json:"dts_time"`
			PTS   string `json:"pts_time"`
			Flags string `json:"flags"`
		} `json:"packets"`
		Streams []struct {
			Type       string `json:"codec_type"`
			Rate       string `json:"r_frame_rate"`
			SampleRate string `json:"sample_rate"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return result, err
	}
	metadata, err := mediaCommand(ctx, "ffprobe", "-v", "error", "-count_frames", "-show_entries", "stream=codec_type,nb_read_frames:format=duration", "-of", "json", filename)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(metadata, &result.Media); err != nil {
		return result, err
	}
	for _, stream := range probe.Streams {
		if (stream.Type == "video" && stream.Rate != "25/1") || (stream.Type == "audio" && stream.SampleRate != "44100") {
			return result, errors.New("takeover verifier requires the generated 25fps/44.1kHz fixtures")
		}
	}
	for _, packet := range probe.Packets {
		decoding, err := timestampUS(packet.DTS)
		if err != nil {
			return result, err
		}
		presentation, err := timestampUS(packet.PTS)
		if err != nil {
			return result, err
		}
		if packet.Type != "video" && packet.Type != "audio" {
			return result, errors.New("unexpected media track")
		}
		result.Tracks[packet.Type] = append(result.Tracks[packet.Type], recordingPacket{DTS: decoding, PTS: presentation, Key: strings.HasPrefix(packet.Flags, "K")})
	}
	for _, stream := range result.Media.Streams {
		frames, err := strconv.Atoi(stream.Frames)
		if err != nil || frames != len(result.Tracks[stream.CodecType]) {
			return result, errors.New("decoded-frame and packet counts differ")
		}
	}
	if len(result.Video) != len(result.Tracks["video"]) || len(result.Audio) != len(result.Tracks["audio"]) {
		return result, errors.New("digest and packet counts differ")
	}
	return result, nil
}

func absolute(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func verifyTakeoverRecordings(files []recordingIdentity, initial, final recordingIdentity, audioTolerance int64) ([]recordingIdentityResult, error) {
	if len(files) == 0 || len(initial.Video) == 0 || len(final.Video) == 0 || len(final.Audio) == 0 {
		return nil, errors.New("missing recording/source identity")
	}
	for _, source := range []recordingIdentity{initial, final} {
		if len(source.Video) != len(source.Tracks["video"]) || len(source.Audio) != len(source.Tracks["audio"]) {
			return nil, errors.New("source identity lacks matching packet clocks")
		}
	}
	hashes := map[string][]string{"video": {}, "audio": {}}
	for _, file := range files {
		hashes["video"] = append(hashes["video"], file.Video...)
		hashes["audio"] = append(hashes["audio"], file.Audio...)
	}
	videoPrefix, audioPrefix := len(hashes["video"])-len(final.Video), len(hashes["audio"])-len(final.Audio)
	audioSkip := 0
	for audioSkip < len(initial.Tracks["audio"]) && initial.Tracks["audio"][audioSkip].DTS < initial.Tracks["video"][0].DTS {
		audioSkip++
	}
	prefixes := map[string]int{"video": videoPrefix, "audio": audioPrefix}
	starts := map[string]int{"video": 0, "audio": audioSkip}
	references := map[string][]recordingPacket{}
	for _, track := range []string{"video", "audio"} {
		oldHashes, newHashes := initial.Video, final.Video
		if track == "audio" {
			oldHashes, newHashes = initial.Audio, final.Audio
		}
		prefix, start := prefixes[track], starts[track]
		if prefix <= 0 || start+prefix > len(oldHashes) || len(oldHashes) != len(initial.Tracks[track]) || len(newHashes) != len(final.Tracks[track]) {
			return nil, fmt.Errorf("invalid %s retained prefix", track)
		}
		expected := append(append([]string{}, oldHashes[start:start+prefix]...), newHashes...)
		for index, digest := range hashes[track] {
			if digest != expected[index] {
				return nil, fmt.Errorf("%s payload mismatch at %d", track, index)
			}
		}
		references[track] = append(append([]recordingPacket{}, initial.Tracks[track][start:start+prefix]...), final.Tracks[track]...)
	}
	positions := map[string]int{"video": 0, "audio": 0}
	var results []recordingIdentityResult
	for fileIndex, file := range files {
		video := file.Tracks["video"]
		if len(video) != len(file.Video) || len(file.Tracks["audio"]) != len(file.Audio) || len(video) == 0 || len(file.Audio) == 0 || !video[0].Key || video[0].DTS != 0 {
			return nil, fmt.Errorf("%s: invalid first keyframe/counts", file.File)
		}
		offsets := map[int]int64{}
		result := recordingIdentityResult{File: file.File, SHA256: file.SHA256, VideoFrames: len(video), AudioPackets: len(file.Audio), Duration: file.Media.Format.Duration}
		minimumPTS, maximumEnd := int64(math.MaxInt64), int64(math.MinInt64)
		minimumDTS, maximumDTS := int64(math.MaxInt64), int64(math.MinInt64)
		for _, track := range []string{"video", "audio"} {
			for index, packet := range file.Tracks[track] {
				global := positions[track] + index
				if global >= len(references[track]) {
					return nil, errors.New("packet count exceeds reference")
				}
				source := references[track][global]
				generation := 0
				if global >= prefixes[track] {
					generation = 1
				}
				offset, exists := offsets[generation]
				if !exists && track == "video" {
					offset = packet.DTS - source.DTS
					offsets[generation] = offset
					exists = true
				}
				if !exists {
					return nil, errors.New("audio generation has no matching video clock")
				}
				tolerance, duration := int64(1), int64(40000)
				if track == "audio" {
					tolerance, duration = audioTolerance, 23220
					result.MaximumAudioErrorUS = max(result.MaximumAudioErrorUS, absolute(packet.DTS-source.DTS-offset))
				}
				if absolute(packet.DTS-source.DTS-offset) > tolerance || absolute(packet.PTS-source.PTS-offset) > tolerance || (track == "video" && packet.Key != source.Key) {
					return nil, fmt.Errorf("%s %s packet %d: source clock/keyframe changed (error=%d us)", file.File, track, index, packet.DTS-source.DTS-offset)
				}
				if index > 0 && packet.DTS <= file.Tracks[track][index-1].DTS {
					return nil, fmt.Errorf("%s: non-increasing %s DTS", file.File, track)
				}
				if track == "video" && index > 0 && packet.Key && packet.DTS >= 4000000 {
					return nil, errors.New("recording skipped an eligible keyframe")
				}
				minimumPTS = min(minimumPTS, packet.PTS)
				maximumEnd = max(maximumEnd, packet.PTS+duration)
				minimumDTS = min(minimumDTS, packet.DTS)
				maximumDTS = max(maximumDTS, packet.DTS)
			}
		}
		result.SourceTransition = len(offsets) == 2
		if result.SourceTransition {
			transition := videoPrefix - positions["video"]
			if transition <= 0 || transition >= len(video) || video[transition].DTS-video[transition-1].PTS > 73220 || video[transition].DTS <= video[transition-1].DTS {
				return nil, errors.New("takeover introduces a video timestamp gap")
			}
		}
		nextIndex := positions["video"] + len(video)
		if fileIndex+1 < len(files) {
			generation := 0
			if nextIndex >= videoPrefix {
				generation = 1
			}
			offset, exists := offsets[generation]
			if !exists || nextIndex >= len(references["video"]) || !references["video"][nextIndex].Key {
				return nil, errors.New("next recording lacks a source-mapped keyframe")
			}
			boundary := references["video"][nextIndex].DTS + offset
			if boundary < 4000000 {
				return nil, errors.New("recording cut before its target")
			}
			result.NextKeyframeSeconds = float64(boundary) / 1e6
		}
		duration, err := timestampUS(file.Media.Format.Duration)
		if err != nil || duration < maximumDTS-minimumDTS-1000 || duration > maximumEnd-minimumPTS+1000 {
			return nil, fmt.Errorf("%s: container duration %d outside source-matched decode/presentation span [%d, %d]", file.File, duration, maximumDTS-minimumDTS, maximumEnd-minimumPTS)
		}
		maximum := 4.5
		if result.SourceTransition {
			maximum = float64(maximumEnd-minimumPTS+1000) / 1e6
		}
		if err := file.Media.validate(maximum); err != nil {
			return nil, err
		}
		positions["video"] += len(video)
		positions["audio"] += len(file.Audio)
		results = append(results, result)
	}
	return results, nil
}

func (harness *harness) takeoverRecordingIdentity() error {
	references := make([]recordingIdentity, 2)
	for index, fixture := range []string{"regular", "bframes"} {
		seconds := []string{"70", "40"}[index]
		filename := filepath.Join(harness.evidence, "reference-"+fixture+".flv")
		if _, err := mediaCommand(harness.ctx, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-y", "-stream_loop", "-1", "-i", filepath.Join(harness.root, "testdata", "generated", fixture+".flv"), "-t", seconds, "-c", "copy", "-f", "flv", filename); err != nil {
			return err
		}
		identity, err := inspectRecording(harness.ctx, filename)
		if err != nil {
			return err
		}
		references[index] = identity
	}
	harness.results["recording_identity_references"] = []map[string]any{}
	for _, source := range references {
		harness.results["recording_identity_references"] = append(harness.results["recording_identity_references"].([]map[string]any), map[string]any{"file": source.File, "sha256": source.SHA256, "video_frames": len(source.Video), "audio_packets": len(source.Audio)})
	}
	verified := map[string]any{}
	harness.results["recording_identity"] = verified
	for _, app := range []string{"flv", "ts"} {
		files, err := recordingFiles(filepath.Join(harness.evidence, "recordings", app))
		if err != nil {
			return err
		}
		if len(files) < 8 {
			return errors.New("takeover has fewer than eight recording files")
		}
		var identities []recordingIdentity
		for _, filename := range files {
			identity, err := inspectRecording(harness.ctx, filename)
			if err != nil {
				return fmt.Errorf("%s: %w", filename, err)
			}
			identities = append(identities, identity)
		}
		tolerance := int64(1)
		if app == "ts" {
			tolerance = 10001
		}
		result, err := verifyTakeoverRecordings(identities, references[0], references[1], tolerance)
		if err != nil {
			return fmt.Errorf("%s recordings: %w", app, err)
		}
		verified[app] = result
	}
	harness.results["recording_identity_scope"] = "Every retained video frame, AAC payload and per-file PTS/DTS matches the source; all final-source frames/packets present. Old source is a retained prefix after pre-video audio trimming, not a guarantee that every packet sent before forced takeover arrived. Immediate four-second boundaries use the first eligible source keyframe. AAC timing is checked within the existing TS ten-millisecond synchronization bound; this is not decoded PCM seam verification."
	return nil
}
