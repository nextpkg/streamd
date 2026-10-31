package config

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinderLocalHandoffOwnsCacheAndLifetime(t *testing.T) {
	prepare := func(apps ...string) *Finder {
		t.Helper()
		finder := NewConfig().Finder
		names := make(map[string]*App)
		for _, name := range apps {
			names[name] = nil
		}
		require.NoError(t, finder.init(context.Background(), names))
		t.Cleanup(finder.Close)
		return &finder
	}
	previous := prepare("live", "removed")
	previous.Register("live", "sample")
	previous.Register("removed", "sample")
	next := prepare("live", "added")
	next.inheritLocalRegistrations(previous)
	require.Equal(t, "127.0.0.1", next.GetBackend("live", "sample"))
	require.Empty(t, next.GetBackend("removed", "sample"))
	require.Empty(t, next.GetBackend("added", "sample"))
	previous.Deregister("live", "sample")
	previous.Close()
	require.NoError(t, next.runtime.ctx.Err(), "retiring the old generation must not cancel its replacement")
	require.Equal(t, "127.0.0.1", next.GetBackend("live", "sample"), "generations must not share mutable caches")
	for _, redisSource := range []bool{false, true} {
		candidate := prepare("live")
		source := prepare("live")
		source.Register("live", "sample")
		if redisSource {
			source.Redis = &RedisConfig{}
		} else {
			candidate.Redis = &RedisConfig{}
		}
		candidate.inheritLocalRegistrations(source)
		require.Empty(t, candidate.GetBackend("live", "sample"), "Redis generations must load authoritative registrations")
	}
}

func TestUninitializedFinderHasNoRegistrations(t *testing.T) {
	finder := &Finder{}
	require.Empty(t, finder.GetBackend("live", "sample"))
	finder.Register("live", "sample")
	finder.Deregister("live", "sample")
	finder.Close()
	require.Empty(t, finder.GetBackend("live", "sample"))
}
