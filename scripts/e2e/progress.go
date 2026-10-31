package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type frameProgress struct {
	Frames int     `json:"frames"`
	MaxGap float64 `json:"max_gap_seconds"`
	last   time.Time
}

func (progress *frameProgress) observe(now time.Time, frames int, limit time.Duration) error {
	if frames < progress.Frames {
		return errors.New("decoded frame counter moved backwards")
	}
	if !progress.last.IsZero() {
		gap := now.Sub(progress.last)
		progress.MaxGap = max(progress.MaxGap, gap.Seconds())
		if gap > limit {
			return fmt.Errorf("decoded frames stalled for %.3fs", gap.Seconds())
		}
	}
	if frames > progress.Frames {
		progress.last = now
	}
	progress.Frames = frames
	return nil
}

// completeProgressFrames ignores an unfinished line while FFmpeg is still writing.
func completeProgressFrames(body string) (int, error) {
	boundary := strings.LastIndexByte(body, '\n')
	if boundary < 0 {
		return 0, nil
	}
	return parseFrameProgress(body[:boundary])
}

// decodedFrames validates the complete file after the decoder exits.
func decodedFrames(body string) (int, error) {
	if body != "" && !strings.HasSuffix(body, "\n") {
		return 0, errors.New("incomplete decoder progress line")
	}
	frames, err := parseFrameProgress(body)
	if err != nil {
		return 0, err
	}
	if frames == 0 {
		return 0, errors.New("no decoded video frames")
	}
	return frames, nil
}

func parseFrameProgress(body string) (int, error) {
	last := 0
	for _, line := range strings.Split(body, "\n") {
		if value, exists := strings.CutPrefix(line, "frame="); exists {
			frames, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil || frames < 0 {
				return 0, fmt.Errorf("invalid frame count %q", value)
			}
			if frames < last {
				return 0, errors.New("decoded frame counter moved backwards")
			}
			last = frames
		}
	}
	return last, nil
}

type playbackWatch struct {
	ready    chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
	progress map[string]*frameProgress
	trace    []map[string]any
	err      error
}

func (harness *harness) watchPlayback(players map[string]*process) *playbackWatch {
	ctx, cancel := context.WithCancel(harness.ctx)
	watch := &playbackWatch{ready: make(chan struct{}), done: make(chan struct{}), cancel: cancel, progress: make(map[string]*frameProgress)}
	go func() {
		defer close(watch.done)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		started, ready := time.Now(), false
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				warmed := true
				for name, child := range players {
					progress := watch.progress[name]
					if progress == nil {
						progress = &frameProgress{}
						watch.progress[name] = progress
					}
					body, err := os.ReadFile(filepath.Join(harness.evidence, "progress-"+name+".txt"))
					if err != nil && !os.IsNotExist(err) {
						watch.err = err
						return
					}
					frames, err := completeProgressFrames(string(body))
					if err != nil {
						watch.err = err
						return
					}
					if !child.alive() {
						if child.err != nil {
							watch.err = fmt.Errorf("%s exited: %w", name, child.err)
							return
						}
						if frames == 0 {
							watch.err = fmt.Errorf("%s exited without decoding frames", name)
							return
						}
						continue
					}
					if err := progress.observe(now, frames, 5*time.Second); err != nil {
						watch.err = fmt.Errorf("%s: %w", name, err)
						return
					}
					if frames == 0 {
						warmed = false
					}
					watch.trace = append(watch.trace, map[string]any{"elapsed": now.Sub(started).Seconds(), "player": name, "pid": child.command.Process.Pid, "frames": frames})
				}
				if warmed && !ready {
					close(watch.ready)
					ready = true
				}
				if !ready && now.Sub(started) > 12*time.Second {
					watch.err = errors.New("players did not start decoding")
					return
				}
			}
		}
	}()
	return watch
}

func (watch *playbackWatch) finish(harness *harness) error {
	watch.cancel()
	<-watch.done
	harness.results["playback_continuity"] = map[string]any{"sampling_seconds": 0.25, "maximum_allowed_gap_seconds": 5, "players": watch.progress, "trace": watch.trace, "passed": watch.err == nil}
	return watch.err
}
