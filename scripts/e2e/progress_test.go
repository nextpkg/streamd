package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompleteProgressFramesHandlesPartialWrites(t *testing.T) {
	for _, sample := range []struct {
		body   string
		frames int
	}{
		{"", 0}, {"frame=0\n", 0}, {"frame=3", 0}, {"frame=3\nframe=4", 3}, {"frame= 14\nprogress=continue\nframe=", 14},
	} {
		frames, err := completeProgressFrames(sample.body)
		require.NoError(t, err)
		require.Equal(t, sample.frames, frames)
	}
	for _, malformed := range []string{"frame=-1\n", "frame=oops\n", "frame=\n"} {
		_, err := completeProgressFrames(malformed)
		require.Error(t, err)
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
