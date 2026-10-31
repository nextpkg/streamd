package appstate

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackgroundApplicationFailurePreservesOwnerAndCurrentState(t *testing.T) {
	stream := t.Name()
	Start("recording", stream, "current", 12, "active")
	defer Forget("recording", stream, "current")
	ReportBackgroundFailure("recording", stream, "previous")
	var workers sync.WaitGroup
	for index := 0; index < 100; index++ {
		workers.Go(func() { ReportBackgroundFailure("recording", stream, "current") })
		workers.Go(func() { Report("recording", stream, "current", 13, "disabled") })
	}
	workers.Wait()
	value, exists := applications.Load("recording:" + stream)
	require.True(t, exists)
	state := value.(Status)
	require.Equal(t, uint64(100), state.Failures)
	require.Positive(t, state.LastFailureAt)
	require.Equal(t, uint64(13), state.Revision)
	require.Equal(t, "disabled", state.State)
	Forget("recording", stream, "current")
	ReportBackgroundFailure("recording", stream, "current")
	_, exists = applications.Load("recording:" + stream)
	require.False(t, exists)
}

func TestApplicationStatusOwnership(t *testing.T) {
	stream := t.Name()
	Start("hls", stream, "old", 1, "active")
	Start("hls", stream, "new", 2, "waiting_keyframe")
	Report("hls", stream, "old", 1, "active")
	Forget("hls", stream, "old")
	value, exists := applications.Load("hls:" + stream)
	require.True(t, exists)
	require.Equal(t, uint64(2), value.(Status).Revision)
	ReportFailure("hls", stream, "new", 2)
	Report("hls", stream, "new", 2, "active")
	value, _ = applications.Load("hls:" + stream)
	require.Equal(t, uint64(1), value.(Status).Failures)
	require.Positive(t, value.(Status).LastFailureAt)
	Forget("hls", stream, "new")
	_, exists = applications.Load("hls:" + stream)
	require.False(t, exists)
}
