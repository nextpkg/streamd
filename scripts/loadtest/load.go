package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type clientResult struct {
	Index    int           `json:"index"`
	PID      int           `json:"pid,omitempty"`
	Joined   bool          `json:"joined"`
	Passed   bool          `json:"passed"`
	Error    string        `json:"error,omitempty"`
	Elapsed  float64       `json:"elapsed_seconds"`
	Progress mediaProgress `json:"progress"`
}

func ffmpegArguments(settings options, target string, paced bool) []string {
	arguments := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-y", "-stats_period", "0.25", "-progress", "pipe:1"}
	if settings.mode == "publish" || settings.mode == "slab" {
		arguments = append(arguments, "-stream_loop", "-1")
		if paced {
			arguments = append(arguments, "-re")
		}
		arguments = append(arguments, "-i", settings.input)
	} else {
		arguments = append(arguments, "-i", target)
	}
	if settings.duration > 0 {
		arguments = append(arguments, "-t", strconv.FormatFloat(settings.duration.Seconds(), 'f', 6, 64))
	}
	arguments = append(arguments, "-map", "0:v:0", "-map", "0:a:0?", "-c", "copy", "-f", "flv")
	if settings.mode == "play" {
		return append(arguments, os.DevNull)
	}
	return append(arguments, target)
}

func runMedia(ctx context.Context, settings options, target, filename string, paced bool) clientResult {
	started := time.Now()
	progress := &progressWriter{}
	process, err := startChild(settings.ffmpeg, ffmpegArguments(settings, target, paced), nil, progress, filename)
	result := clientResult{}
	if err == nil {
		result.PID = process.command.Process.Pid
		if settings.duration > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, time.Now().Add(settings.duration).Add(30*time.Second))
			defer cancel()
		}
		err = process.wait(ctx)
		result.Joined = true
		result.Progress = progress.snapshot()
		body, readErr := os.ReadFile(filename)
		err = errors.Join(err, readErr)
		if len(body) != 0 {
			err = errors.Join(err, errors.New("FFmpeg reported errors; inspect the client log"))
		}
		if result.Progress.Frames <= 0 || result.Progress.Microseconds <= 0 || !result.Progress.End {
			err = errors.Join(err, errors.New("FFmpeg did not confirm nonempty media completion"))
		}
		if settings.duration > 0 && time.Duration(result.Progress.Microseconds)*time.Microsecond < settings.duration-time.Second {
			err = errors.Join(err, errors.New("media ended before the requested duration"))
		}
	}
	result.Elapsed, result.Passed = time.Since(started).Seconds(), err == nil
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func generatePreset(ctx context.Context, settings options, directory string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	filename := filepath.Join(directory, "preset-"+settings.bitrate+".flv")
	arguments := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-n", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25", "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=44100", "-t", "6", "-map", "0:v:0", "-map", "1:a:0", "-c:v", "libx264", "-preset", "veryfast", "-threads:v", "1", "-pix_fmt", "yuv420p", "-profile:v", "main", "-bf", "0", "-g", "50", "-keyint_min", "50", "-sc_threshold", "0", "-b:v", settings.bitrate, "-c:a", "aac", "-b:a", "32k", "-map_metadata", "-1", "-f", "flv", filename}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	logPath := filepath.Join(directory, "fixture.log")
	process, err := startChild(settings.ffmpeg, arguments, nil, io.Discard, logPath)
	if err != nil {
		return "", err
	}
	if err := process.wait(ctx); err != nil {
		return "", err
	}
	body, err := os.ReadFile(logPath)
	if err != nil || len(body) != 0 {
		return "", errors.Join(err, errors.New("fixture generation reported errors"))
	}
	info, err := os.Stat(filename)
	if err != nil || info.Size() == 0 {
		return "", errors.Join(err, errors.New("fixture generation produced no media"))
	}
	return filename, nil
}

func runLoad(ctx context.Context, settings options, directory string) ([]clientResult, error) {
	if settings.mode == "publish" && settings.input == "" {
		filename, err := generatePreset(ctx, settings, directory)
		if err != nil {
			return nil, err
		}
		settings.input = filename
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	results := make([]clientResult, settings.concurrency)
	var workers sync.WaitGroup
	for index := range results {
		workers.Add(1)
		go func() {
			defer workers.Done()
			results[index].Index = index + 1
			delay := time.Duration(0)
			if settings.stagger > 0 {
				delay = time.Duration(rand.Int64N(int64(settings.stagger)))
			}
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				results[index].Error = ctx.Err().Error()
				return
			case <-timer.C:
			}
			if err := ctx.Err(); err != nil {
				results[index].Error = err.Error()
				return
			}
			suffix := 0
			if settings.mode == "publish" {
				suffix = index + 1
			}
			target, err := streamURL(settings.target, suffix)
			if err != nil {
				results[index].Error = err.Error()
				cancel(err)
				return
			}
			results[index] = runMedia(ctx, settings, target, filepath.Join(directory, fmt.Sprintf("client-%05d.log", index+1)), true)
			results[index].Index = index + 1
			if !results[index].Passed {
				cancel(fmt.Errorf("client %d: %s", index+1, results[index].Error))
			}
		}()
	}
	workers.Wait()
	return results, context.Cause(ctx)
}
