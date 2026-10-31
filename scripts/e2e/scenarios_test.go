package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestScenarioCatalogPreservesExistingEntrypoints(t *testing.T) {
	expected := map[string][2]string{
		"admin":                    {"admin-statistics-", "latest-admin-statistics.txt"},
		"applications":             {"application-lifecycle-", "latest-application-lifecycle.txt"},
		"callback-stress":          {"callback-stress-", "latest-callback-stress.txt"},
		"callback-takeover":        {"callback-takeover-", "latest-callback-takeover.txt"},
		"callbacks":                {"callbacks-e2e-", "latest-callback-evidence.txt"},
		"cloud-container":          {"cloud-container-", "latest-cloud-container.txt"},
		"cloud-crash":              {"cloud-crash-", "latest-cloud-crash.txt"},
		"cloud-resume":             {"cloud-resume-", "latest-cloud-resume.txt"},
		"completion-outcome":       {"completion-outcome-", "latest-completion-outcome.txt"},
		"filesystem-fault":         {"filesystem-fault-", "latest-filesystem-fault.txt"},
		"fragment-clock":           {"fragment-clock-", "latest-fragment-clock.txt"},
		"hls-origin":               {"hls-origin-", "latest-hls-origin-evidence.txt"},
		"joint-lifecycle":          {"joint-lifecycle-", "latest-joint-lifecycle.txt"},
		"load-tools":               {"load-tools-", "latest-load-tools.txt"},
		"metrics":                  {"metrics-", "latest-metrics.txt"},
		"origin":                   {"origin-e2e-", "latest-origin-evidence.txt"},
		"origin-fault":             {"origin-fault-", "latest-origin-fault-evidence.txt"},
		"passerby":                 {"passerby-", "latest-passerby.txt"},
		"proxy":                    {"proxy-e2e-", "latest-proxy-evidence.txt"},
		"recording-crash":          {"recording-crash-", "latest-recording-crash.txt"},
		"recording-credentials":    {"recording-credentials-", "latest-recording-credentials.txt"},
		"recording-targets":        {"recording-targets-", "latest-recording-targets.txt"},
		"redis":                    {"redis-e2e-", "latest-redis-evidence.txt"},
		"redis-prepare":            {"redis-prepare-", "latest-redis-prepare.txt"},
		"relay":                    {"relay-e2e-", "latest-relay-evidence.txt"},
		"release":                  {"release-", "latest-release.txt"},
		"reload":                   {"media-reload-", "latest-media-reload.txt"},
		"republish":                {"republish-", "latest-republish.txt"},
		"resources":                {"resources-", "latest-resources.txt"},
		"resume":                   {"round-resume-", "latest-round-resume.txt"},
		"rotation":                 {"rotation-", "latest-rotation.txt"},
		"round-expiry":             {"round-expiry-", "latest-round-expiry.txt"},
		"rtmp-lifecycle":           {"rtmp-lifecycle-", "latest-rtmp-lifecycle.txt"},
		"slow-consumer":            {"slow-consumer-", "latest-slow-consumer.txt"},
		"slow-hls":                 {"slow-hls-", "latest-slow-hls.txt"},
		"stability":                {"stability-", "latest-stability.txt"},
		"static-push":              {"static-push-", "latest-static-push.txt"},
		"storage-fault":            {"storage-fault-", "latest-storage-fault.txt"},
		"storage-inspection":       {"storage-inspection-", "latest-storage-inspection.txt"},
		"storage-inspection-retry": {"storage-inspection-retry-", "latest-storage-inspection-retry.txt"},
		"storage-lifecycle":        {"storage-lifecycle-", "latest-storage-lifecycle.txt"},
		"stream":                   {"stream-e2e-", "latest-stream-evidence.txt"},
		"upload-fault":             {"upload-fault-", "latest-upload-fault.txt"},
	}
	require.Len(t, scenarios, len(expected))
	seen := map[string]bool{}
	for name, locations := range expected {
		spec, exists := scenarios[name]
		require.True(t, exists, name)
		require.NotNil(t, spec.Run, name)
		require.Equal(t, locations[0], spec.Prefix, name)
		require.Equal(t, locations[1], spec.Marker, name)
		require.False(t, seen[spec.Marker], "duplicate marker")
		seen[spec.Marker] = true
	}
	for _, names := range suites {
		for _, name := range names {
			_, ok := scenarios[name]
			require.True(t, ok, name)
		}
	}
}
