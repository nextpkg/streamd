package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlaybackReadinessRequiresLiveWatch(t *testing.T) {
	failure := errors.New("decoder failed")
	for _, tc := range []struct {
		name                      string
		ready, stopped, cancelled bool
		failure                   error
		want                      error
	}{
		{"ready", true, false, false, nil, nil},
		{"decoder-failed", false, true, false, failure, failure},
		{"cancelled", false, false, true, nil, context.Canceled},
		{"ready-but-cancelled", true, false, true, nil, context.Canceled},
		{"ready-but-failed", true, true, false, failure, failure},
		{"stopped-without-error", false, true, false, nil, nil},
		{"ready-but-stopped", true, true, false, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			watch := &playbackWatch{ready: make(chan struct{}), done: make(chan struct{}), err: tc.failure}
			if tc.ready {
				close(watch.ready)
			}
			if tc.stopped {
				close(watch.done)
			}
			if tc.cancelled {
				cancel()
			}
			err := watch.waitReady(ctx)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
			} else if tc.stopped {
				require.ErrorContains(t, err, "stopped")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDecodedResultRules(t *testing.T) {
	for _, tc := range []struct {
		name, body      string
		minimum, frames int
		wantError       string
	}{
		{"at-minimum", "frame=100\n", 100, 100, ""},
		{"above-minimum", "frame=101\n", 100, 101, ""},
		{"below-minimum", "frame=99\n", 100, 99, "need at least 100"},
		{"zero-threshold-does-not-accept-no-video", "frame=0\n", 0, 0, "no decoded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frames, err := validateDecodedFrames(tc.body, tc.minimum)
			require.Equal(t, tc.frames, frames)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Exercise the adapter with completed processes and real evidence files, without
// launching FFmpeg or duplicating a full streaming scenario for each bad result.
func TestFinishPlayersRequiresProcessAndMediaEvidence(t *testing.T) {
	failure := errors.New("decoder process failed")
	for _, tc := range []struct {
		name           string
		processError   error
		missing, short bool
		wantError      string
	}{
		{"success", nil, false, false, ""},
		{"exit-failure-with-valid-frames", failure, false, false, "decoder process failed"},
		{"missing-progress", nil, true, false, ""},
		{"insufficient-frames", nil, false, true, "need at least 100"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &harness{ctx: context.Background(), evidence: t.TempDir(), results: map[string]any{}}
			child := &process{done: make(chan struct{}), err: tc.processError}
			close(child.done)
			if !tc.missing {
				body := "frame=100\n"
				if tc.short {
					body = "frame=99\n"
				}
				require.NoError(t, os.WriteFile(filepath.Join(h.evidence, "progress-live-hls.txt"), []byte(body), 0600))
			}
			err := h.finishPlayers(map[string]*process{"live-hls": child}, 100)
			if tc.missing {
				require.ErrorIs(t, err, os.ErrNotExist)
				return
			}
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 0, h.results["play-live-hls"])
			require.Equal(t, 100, h.results["decoded_frames-live-hls"])
		})
	}
}
