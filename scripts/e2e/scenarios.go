package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
)

type scenarioSpec struct {
	Prefix string               `json:"prefix"`
	Marker string               `json:"marker"`
	Run    func(*harness) error `json:"-"`
}

// Keep CLI dispatch and evidence locations in one place. Existing scenario names
// and artifact markers remain compatible with external scripts.
var scenarios = map[string]scenarioSpec{
	"admin":                    {"admin-statistics-", "latest-admin-statistics.txt", (*harness).adminStatistics},
	"applications":             {"application-lifecycle-", "latest-application-lifecycle.txt", (*harness).applicationLifecycle},
	"callback-stress":          {"callback-stress-", "latest-callback-stress.txt", (*harness).callbackStress},
	"callback-takeover":        {"callback-takeover-", "latest-callback-takeover.txt", (*harness).callbackTakeover},
	"callbacks":                {"callbacks-e2e-", "latest-callback-evidence.txt", (*harness).callbackMedia},
	"cloud-container":          {"cloud-container-", "latest-cloud-container.txt", func(h *harness) error { return h.cloudResume(true) }},
	"cloud-crash":              {"cloud-crash-", "latest-cloud-crash.txt", (*harness).cloudCrash},
	"cloud-resume":             {"cloud-resume-", "latest-cloud-resume.txt", func(h *harness) error { return h.cloudResume(false) }},
	"completion-outcome":       {"completion-outcome-", "latest-completion-outcome.txt", (*harness).completionOutcome},
	"filesystem-fault":         {"filesystem-fault-", "latest-filesystem-fault.txt", (*harness).filesystemFault},
	"fragment-clock":           {"fragment-clock-", "latest-fragment-clock.txt", (*harness).fragmentClock},
	"hls-origin":               {"hls-origin-", "latest-hls-origin-evidence.txt", (*harness).hlsOrigin},
	"joint-lifecycle":          {"joint-lifecycle-", "latest-joint-lifecycle.txt", (*harness).jointLifecycle},
	"load-tools":               {"load-tools-", "latest-load-tools.txt", (*harness).loadTools},
	"metrics":                  {"metrics-", "latest-metrics.txt", (*harness).metricsMedia},
	"origin":                   {"origin-e2e-", "latest-origin-evidence.txt", (*harness).originMedia},
	"origin-fault":             {"origin-fault-", "latest-origin-fault-evidence.txt", (*harness).originFault},
	"passerby":                 {"passerby-", "latest-passerby.txt", (*harness).passerbyMedia},
	"proxy":                    {"proxy-e2e-", "latest-proxy-evidence.txt", (*harness).proxyMedia},
	"recording-crash":          {"recording-crash-", "latest-recording-crash.txt", (*harness).recordingCrash},
	"recording-credentials":    {"recording-credentials-", "latest-recording-credentials.txt", (*harness).recordingCredentials},
	"recording-targets":        {"recording-targets-", "latest-recording-targets.txt", (*harness).recordingTargets},
	"redis":                    {"redis-e2e-", "latest-redis-evidence.txt", (*harness).redisDiscovery},
	"redis-prepare":            {"redis-prepare-", "latest-redis-prepare.txt", (*harness).redisPrepareMedia},
	"relay":                    {"relay-e2e-", "latest-relay-evidence.txt", (*harness).relayMedia},
	"release":                  {"release-", "latest-release.txt", (*harness).releaseMedia},
	"reload":                   {"media-reload-", "latest-media-reload.txt", (*harness).reload},
	"republish":                {"republish-", "latest-republish.txt", (*harness).republish},
	"resources":                {"resources-", "latest-resources.txt", (*harness).resourceBaseline},
	"resume":                   {"round-resume-", "latest-round-resume.txt", (*harness).resume},
	"rotation":                 {"rotation-", "latest-rotation.txt", (*harness).rotationMedia},
	"round-expiry":             {"round-expiry-", "latest-round-expiry.txt", (*harness).roundExpiry},
	"rtmp-lifecycle":           {"rtmp-lifecycle-", "latest-rtmp-lifecycle.txt", (*harness).rtmpLifecycle},
	"slow-consumer":            {"slow-consumer-", "latest-slow-consumer.txt", (*harness).slowConsumers},
	"slow-hls":                 {"slow-hls-", "latest-slow-hls.txt", (*harness).slowHLS},
	"stability":                {"stability-", "latest-stability.txt", (*harness).releaseMedia},
	"static-push":              {"static-push-", "latest-static-push.txt", (*harness).staticPushMedia},
	"storage-fault":            {"storage-fault-", "latest-storage-fault.txt", (*harness).storageFault},
	"storage-inspection":       {"storage-inspection-", "latest-storage-inspection.txt", func(h *harness) error { return h.storageInspection(false, false) }},
	"storage-inspection-retry": {"storage-inspection-retry-", "latest-storage-inspection-retry.txt", func(h *harness) error { return h.storageInspection(false, true) }},
	"storage-lifecycle":        {"storage-lifecycle-", "latest-storage-lifecycle.txt", func(h *harness) error { return h.storageInspection(true, false) }},
	"stream":                   {"stream-e2e-", "latest-stream-evidence.txt", runStreamScenario},
	"upload-fault":             {"upload-fault-", "latest-upload-fault.txt", (*harness).uploadFault},
}

func scenarioNames() []string {
	names := make([]string, 0, len(scenarios))
	for name := range scenarios {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func streamDuration(value string) (int, error) {
	if value == "" {
		return 60, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds < 45 {
		return 0, fmt.Errorf("STREAMD_E2E_SECONDS must be an integer of at least 45")
	}
	return seconds, nil
}

func runStreamScenario(h *harness) error {
	seconds, err := streamDuration(os.Getenv("STREAMD_E2E_SECONDS"))
	if err != nil {
		return err
	}
	return h.stream(seconds)
}
