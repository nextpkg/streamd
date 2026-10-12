package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReloadDeduplicatesCanonicalConfiguration(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("application:\n  - enable: true\n    app_name: live\n    static_pull: [rtmp://localhost/one, rtmp://localhost/two, rtmp://localhost/one]\n")
	require.NoError(t, os.WriteFile(filename, data, 0600))
	manager, err := NewManager(context.Background(), filename)
	require.NoError(t, err)
	defer Get().Revoke()
	previous := Get()
	for index := 0; index < 20; index++ {
		require.NoError(t, manager.Reload())
		require.Same(t, previous, Get())
		require.Equal(t, uint64(1), manager.Version())
	}
	require.Equal(t, []string{"rtmp://localhost/one", "rtmp://localhost/two"}, Get().GetStaticPull("live"))
}

func TestApplicationStatusOwnership(t *testing.T) {
	stream := t.Name()
	StartApplication("hls", stream, "old", 1, "active")
	StartApplication("hls", stream, "new", 2, "waiting_keyframe")
	ReportApplication("hls", stream, "old", 1, "active")
	ForgetApplication("hls", stream, "old")
	value, exists := applications.Load("hls:" + stream)
	require.True(t, exists)
	require.Equal(t, uint64(2), value.(ApplicationStatus).Revision)
	ReportApplicationFailure("hls", stream, "new", 2)
	ReportApplication("hls", stream, "new", 2, "active")
	value, _ = applications.Load("hls:" + stream)
	require.Equal(t, uint64(1), value.(ApplicationStatus).Failures)
	require.Positive(t, value.(ApplicationStatus).LastFailureAt)
	ForgetApplication("hls", stream, "new")
	_, exists = applications.Load("hls:" + stream)
	require.False(t, exists)
}
