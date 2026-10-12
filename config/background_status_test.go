package config

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackgroundApplicationFailurePreservesOwnerAndCurrentState(t *testing.T) {
	stream := t.Name()
	StartApplication("recording", stream, "current", 12, "active")
	defer ForgetApplication("recording", stream, "current")
	ReportApplicationBackgroundFailure("recording", stream, "previous")
	var workers sync.WaitGroup
	for index := 0; index < 100; index++ {
		workers.Go(func() { ReportApplicationBackgroundFailure("recording", stream, "current") })
		workers.Go(func() { ReportApplication("recording", stream, "current", 13, "disabled") })
	}
	workers.Wait()
	value, exists := applications.Load("recording:" + stream)
	require.True(t, exists)
	state := value.(ApplicationStatus)
	require.Equal(t, uint64(100), state.Failures)
	require.Positive(t, state.LastFailureAt)
	require.Equal(t, uint64(13), state.Revision)
	require.Equal(t, "disabled", state.State)
	ForgetApplication("recording", stream, "current")
	ReportApplicationBackgroundFailure("recording", stream, "current")
	_, exists = applications.Load("recording:" + stream)
	require.False(t, exists)
}
