package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFrameProgressParsing(t *testing.T) {
	for _, tc := range []struct {
		name, body            string
		frames                int
		liveError, finalError string
	}{
		{"empty", "", 0, "", "no decoded"},
		{"zero", "frame=0\n", 0, "", "no decoded"},
		{"partial-first", "frame=3", 0, "", "incomplete"},
		{"partial-next", "frame=3\nframe=4", 3, "", "incomplete"},
		{"whitespace", "frame= 14\r\nprogress=end\n", 14, "", ""},
		{"repeated", "frame=3\nframe=3\nframe=8\n", 8, "", ""},
		{"negative", "frame=-1\n", 0, "invalid", "invalid"},
		{"not-a-number", "frame=oops\n", 0, "invalid", "invalid"},
		{"missing-value", "frame=\n", 0, "invalid", "invalid"},
		{"overflow", "frame=9999999999999999999999999\n", 0, "invalid", "invalid"},
		{"reset", "frame=100\nframe=2\n", 0, "backwards", "backwards"},
		{"reset-then-recovery", "frame=100\nframe=2\nframe=101\n", 0, "backwards", "backwards"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, parser := range []struct {
				name      string
				parse     func(string) (int, error)
				wantError string
			}{{"live", completeProgressFrames, tc.liveError}, {"final", decodedFrames, tc.finalError}} {
				t.Run(parser.name, func(t *testing.T) {
					frames, err := parser.parse(tc.body)
					if parser.wantError != "" {
						require.ErrorContains(t, err, parser.wantError)
						return
					}
					require.NoError(t, err)
					require.Equal(t, tc.frames, frames)
				})
			}
		})
	}
}

func TestPlaybackProgressBoundaries(t *testing.T) {
	start := time.Unix(100, 0)
	for _, tc := range []struct {
		name      string
		elapsed   time.Duration
		frames    int
		wantError string
	}{
		{"equal-limit-still", 5 * time.Second, 10, ""},
		{"equal-limit-advancing", 5 * time.Second, 20, ""},
		{"stalled", 5*time.Second + time.Nanosecond, 10, "stalled"},
		{"resumed-too-late", 5*time.Second + time.Nanosecond, 20, "stalled"},
		{"counter-reset", time.Second, 9, "backwards"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			progress := &frameProgress{}
			require.NoError(t, progress.observe(start, 10, 5*time.Second))
			err := progress.observe(start.Add(tc.elapsed), tc.frames, 5*time.Second)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.frames, progress.Frames)
			require.Equal(t, tc.elapsed.Seconds(), progress.MaxGap)
		})
	}
}

func TestPlaybackProgressRejectsStallsEvenWhenFramesResume(t *testing.T) {
	started := time.Unix(100, 0)
	progress := &frameProgress{}
	require.NoError(t, progress.observe(started, 10, 5*time.Second))
	require.NoError(t, progress.observe(started.Add(time.Second), 20, 5*time.Second))
	require.NoError(t, progress.observe(started.Add(3*time.Second), 20, 5*time.Second))
	require.InDelta(t, 2, progress.MaxGap, 0.001)
	require.ErrorContains(t, progress.observe(started.Add(7*time.Second), 40, 5*time.Second), "stalled")
	require.ErrorContains(t, progress.observe(started.Add(time.Second), 2, 5*time.Second), "backwards")
}
