package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

type mediaProbe struct {
	Streams []struct {
		CodecType string `json:"codec_type"`
		Frames    string `json:"nb_read_frames"`
	} `json:"streams"`
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

type recordingResult struct {
	File       string      `json:"file"`
	Bytes      int64       `json:"bytes"`
	ReturnCode int         `json:"returncode"`
	Stderr     string      `json:"stderr"`
	Media      *mediaProbe `json:"media,omitempty"`
}

func (media mediaProbe) validate(maximumDuration float64) error {
	codecs := make(map[string]bool)
	for _, stream := range media.Streams {
		frames, err := strconv.Atoi(stream.Frames)
		if err != nil || frames <= 0 {
			return fmt.Errorf("%s has no decoded frames: %q", stream.CodecType, stream.Frames)
		}
		codecs[stream.CodecType] = true
	}
	if len(codecs) != 2 || !codecs["audio"] || !codecs["video"] {
		return fmt.Errorf("recording must contain decoded audio and video: %v", codecs)
	}
	duration, err := strconv.ParseFloat(media.Format.Duration, 64)
	if err != nil || !(duration > 0 && duration <= maximumDuration) {
		return fmt.Errorf("recording duration %q outside (0, %.1f]", media.Format.Duration, maximumDuration)
	}
	return nil
}

func recordingFiles(directory string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && (filepath.Ext(path) == ".flv" || filepath.Ext(path) == ".ts") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func (harness *harness) recordings(directory string, minimum int, probe bool) error {
	return harness.recordingsWithDuration(directory, minimum, probe, 4.5)
}

func (harness *harness) recordingsWithDuration(directory string, minimum int, probe bool, maximumDuration float64) error {
	files, err := recordingFiles(filepath.Join(harness.evidence, directory))
	if err != nil {
		return err
	}
	if len(files) < minimum {
		return fmt.Errorf("%s has %d recordings, need at least %d", directory, len(files), minimum)
	}
	results, _ := harness.results["recordings"].([]recordingResult)
	for _, filename := range files {
		info, err := os.Stat(filename)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(harness.evidence, filename)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(harness.ctx, 15*time.Second)
		command := exec.CommandContext(ctx, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-i", filename, "-f", "null", "-")
		output, decodeError := command.CombinedOutput()
		cancel()
		code := -1
		if command.ProcessState != nil {
			code = command.ProcessState.ExitCode()
		}
		result := recordingResult{File: relative, Bytes: info.Size(), ReturnCode: code, Stderr: string(output)}
		results = append(results, result)
		harness.results["recordings"] = results
		if decodeError != nil || len(output) != 0 || info.Size() == 0 {
			return fmt.Errorf("decode %s: code=%d error=%v output=%s", relative, code, decodeError, output)
		}
		if probe {
			ctx, cancel := context.WithTimeout(harness.ctx, 15*time.Second)
			command := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-count_frames", "-show_entries", "stream=codec_type,nb_read_frames:format=duration", "-of", "json", filename)
			var stderr bytes.Buffer
			command.Stderr = &stderr
			output, probeError := command.Output()
			cancel()
			if probeError != nil || stderr.Len() != 0 {
				return fmt.Errorf("probe %s: %v: %s", relative, probeError, stderr.String())
			}
			var media mediaProbe
			if err := json.Unmarshal(output, &media); err != nil {
				return err
			}
			results[len(results)-1].Media = &media
			if err := media.validate(maximumDuration); err != nil {
				return fmt.Errorf("probe %s: %w", relative, err)
			}
		}
	}
	return nil
}

func (harness *harness) recordingSizes() (map[string]int64, error) {
	files, err := filepath.Glob(filepath.Join(harness.evidence, "recordings-*", "*", "sample", "*"))
	if err != nil {
		return nil, err
	}
	sizes := make(map[string]int64)
	for _, filename := range files {
		info, err := os.Stat(filename)
		if err != nil {
			return nil, err
		}
		if info.Mode().IsRegular() {
			sizes[filename] = info.Size()
		}
	}
	return sizes, nil
}
