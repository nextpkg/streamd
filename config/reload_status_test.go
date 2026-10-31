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
