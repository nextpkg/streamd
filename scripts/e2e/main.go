package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
)

func main() {
	scenario := flag.String("scenario", "stream", "named media scenario; see README and Makefile for available scenarios")
	root := flag.String("root", ".", "streamd module directory")
	race := flag.Bool("race", os.Getenv("STREAMD_E2E_RACE") == "1", "build the service with the race detector")
	inspect := flag.String("inspect-admin-recordings", "", "verify saved admin-scenario recordings against independent source references without starting a service")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *inspect != "" {
		harness := &harness{ctx: ctx, root: *root, evidence: *inspect, results: map[string]any{}}
		if err := harness.takeoverRecordingIdentity(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := json.NewEncoder(os.Stdout).Encode(harness.results); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := execute(ctx, *root, *scenario, *race); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func execute(ctx context.Context, root, scenario string, race bool) (result error) {
	duration := 60
	if scenario == "stream" {
		if configured := os.Getenv("STREAMD_E2E_SECONDS"); configured != "" {
			parsed, err := strconv.Atoi(configured)
			if err != nil || parsed < 45 {
				return errors.New("STREAMD_E2E_SECONDS must be an integer of at least 45")
			}
			duration = parsed
		}
	}
	prefixes := map[string]string{"stream": "stream-e2e-", "reload": "media-reload-", "republish": "republish-", "resume": "round-resume-", "cloud-resume": "cloud-resume-", "cloud-container": "cloud-container-"}
	markers := map[string]string{"stream": "latest-stream-evidence.txt", "reload": "latest-media-reload.txt", "republish": "latest-republish.txt", "resume": "latest-round-resume.txt", "cloud-resume": "latest-cloud-resume.txt", "cloud-container": "latest-cloud-container.txt"}
	prefixes["storage-fault"], markers["storage-fault"] = "storage-fault-", "latest-storage-fault.txt"
	prefixes["storage-inspection"], markers["storage-inspection"] = "storage-inspection-", "latest-storage-inspection.txt"
	prefixes["storage-inspection-retry"], markers["storage-inspection-retry"] = "storage-inspection-retry-", "latest-storage-inspection-retry.txt"
	prefixes["storage-lifecycle"], markers["storage-lifecycle"] = "storage-lifecycle-", "latest-storage-lifecycle.txt"
	prefixes["load-tools"], markers["load-tools"] = "load-tools-", "latest-load-tools.txt"
	prefixes["slow-consumer"], markers["slow-consumer"] = "slow-consumer-", "latest-slow-consumer.txt"
	prefixes["slow-hls"], markers["slow-hls"] = "slow-hls-", "latest-slow-hls.txt"
	prefixes["rtmp-lifecycle"], markers["rtmp-lifecycle"] = "rtmp-lifecycle-", "latest-rtmp-lifecycle.txt"
	prefixes["proxy"], markers["proxy"] = "proxy-e2e-", "latest-proxy-evidence.txt"
	prefixes["callbacks"], markers["callbacks"] = "callbacks-e2e-", "latest-callback-evidence.txt"
	prefixes["admin"], markers["admin"] = "admin-statistics-", "latest-admin-statistics.txt"
	prefixes["applications"], markers["applications"] = "application-lifecycle-", "latest-application-lifecycle.txt"
	prefixes["passerby"], markers["passerby"] = "passerby-", "latest-passerby.txt"
	prefixes["static-push"], markers["static-push"] = "static-push-", "latest-static-push.txt"
	prefixes["recording-crash"], markers["recording-crash"] = "recording-crash-", "latest-recording-crash.txt"
	prefixes["recording-targets"], markers["recording-targets"] = "recording-targets-", "latest-recording-targets.txt"
	prefixes["recording-credentials"], markers["recording-credentials"] = "recording-credentials-", "latest-recording-credentials.txt"
	prefixes["fragment-clock"], markers["fragment-clock"] = "fragment-clock-", "latest-fragment-clock.txt"
	prefixes["metrics"], markers["metrics"] = "metrics-", "latest-metrics.txt"
	prefixes["release"], markers["release"] = "release-", "latest-release.txt"
	prefixes["stability"], markers["stability"] = "stability-", "latest-stability.txt"
	prefixes["resources"], markers["resources"] = "resources-", "latest-resources.txt"
	prefixes["round-expiry"], markers["round-expiry"] = "round-expiry-", "latest-round-expiry.txt"
	prefixes["rotation"], markers["rotation"] = "rotation-", "latest-rotation.txt"
	prefixes["cloud-crash"], markers["cloud-crash"] = "cloud-crash-", "latest-cloud-crash.txt"
	prefixes["joint-lifecycle"], markers["joint-lifecycle"] = "joint-lifecycle-", "latest-joint-lifecycle.txt"
	prefixes["completion-outcome"], markers["completion-outcome"] = "completion-outcome-", "latest-completion-outcome.txt"
	prefixes["redis-prepare"], markers["redis-prepare"] = "redis-prepare-", "latest-redis-prepare.txt"
	prefixes["callback-takeover"], markers["callback-takeover"] = "callback-takeover-", "latest-callback-takeover.txt"
	prefixes["callback-stress"], markers["callback-stress"] = "callback-stress-", "latest-callback-stress.txt"
	prefixes["upload-fault"], markers["upload-fault"] = "upload-fault-", "latest-upload-fault.txt"
	prefixes["filesystem-fault"], markers["filesystem-fault"] = "filesystem-fault-", "latest-filesystem-fault.txt"
	prefixes["redis"], markers["redis"] = "redis-e2e-", "latest-redis-evidence.txt"
	prefixes["relay"], markers["relay"] = "relay-e2e-", "latest-relay-evidence.txt"
	prefixes["origin"], markers["origin"] = "origin-e2e-", "latest-origin-evidence.txt"
	prefixes["hls-origin"], markers["hls-origin"] = "hls-origin-", "latest-hls-origin-evidence.txt"
	prefixes["origin-fault"], markers["origin-fault"] = "origin-fault-", "latest-origin-fault-evidence.txt"
	prefix, exists := prefixes[scenario]
	if !exists {
		return fmt.Errorf("unsupported scenario %q", scenario)
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return fmt.Errorf("run from the streamd module or provide -root: %w", err)
	}
	harness, err := newHarness(ctx, root, prefix, markers[scenario], race)
	if err != nil {
		return err
	}
	defer func() {
		harness.cleanup()
		if result != nil {
			harness.results["failure"] = result.Error()
		}
		harness.results["complete"] = result == nil
		result = errors.Join(result, writeJSON(filepath.Join(harness.evidence, "results.json"), harness.results))
		fmt.Println(harness.evidence)
	}()
	harness.results["scenario"] = scenario
	switch scenario {
	case "joint-lifecycle":
		return harness.jointLifecycle()
	case "cloud-crash":
		return harness.cloudCrash()
	case "rotation":
		return harness.rotationMedia()
	case "completion-outcome":
		return harness.completionOutcome()
	case "round-expiry":
		return harness.roundExpiry()
	case "resources":
		return harness.resourceBaseline()
	case "release", "stability":
		return harness.releaseMedia()
	case "redis-prepare":
		return harness.redisPrepareMedia()
	case "metrics":
		return harness.metricsMedia()
	case "fragment-clock":
		return harness.fragmentClock()
	case "recording-credentials":
		return harness.recordingCredentials()
	case "recording-targets":
		return harness.recordingTargets()
	case "recording-crash":
		return harness.recordingCrash()
	case "static-push":
		return harness.staticPushMedia()
	case "passerby":
		return harness.passerbyMedia()
	case "applications":
		return harness.applicationLifecycle()
	case "admin":
		return harness.adminStatistics()
	case "stream":
		return harness.stream(duration)
	case "reload":
		return harness.reload()
	case "republish":
		return harness.republish()
	case "resume":
		return harness.resume()
	case "cloud-resume":
		return harness.cloudResume(false)
	case "cloud-container":
		return harness.cloudResume(true)
	case "storage-fault":
		return harness.storageFault()
	case "storage-inspection":
		return harness.storageInspection(false, false)
	case "storage-inspection-retry":
		return harness.storageInspection(false, true)
	case "storage-lifecycle":
		return harness.storageInspection(true, false)
	case "load-tools":
		return harness.loadTools()
	case "slow-consumer":
		return harness.slowConsumers()
	case "slow-hls":
		return harness.slowHLS()
	case "rtmp-lifecycle":
		return harness.rtmpLifecycle()
	case "proxy":
		return harness.proxyMedia()
	case "callbacks":
		return harness.callbackMedia()
	case "callback-takeover":
		return harness.callbackTakeover()
	case "callback-stress":
		return harness.callbackStress()
	case "upload-fault":
		return harness.uploadFault()
	case "filesystem-fault":
		return harness.filesystemFault()
	case "redis":
		return harness.redisDiscovery()
	case "relay":
		return harness.relayMedia()
	case "origin":
		return harness.originMedia()
	case "hls-origin":
		return harness.hlsOrigin()
	case "origin-fault":
		return harness.originFault()
	}
	return nil
}
