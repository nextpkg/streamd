package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var resourceMetrics = []string{
	"process_cpu_seconds_total", "process_resident_memory_bytes", "process_open_fds",
	"process_network_receive_bytes_total", "process_network_transmit_bytes_total",
	"go_goroutines", "go_memstats_heap_alloc_bytes", "go_memstats_heap_inuse_bytes",
	"go_memstats_heap_objects", "go_memstats_alloc_bytes_total", "go_memstats_mallocs_total",
	"go_memstats_frees_total", "go_gc_duration_seconds_count",
}

type resourceSample struct {
	Seconds       float64            `json:"seconds"`
	Values        map[string]float64 `json:"metrics"`
	HTTPSeconds   float64            `json:"hls_get_seconds,omitempty"`
	HTTPBodyBytes int                `json:"hls_get_bytes,omitempty"`
}

func parseResources(body string) (map[string]float64, error) {
	values := map[string]float64{}
	for _, line := range strings.Split(body, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		for _, name := range resourceMetrics {
			if fields[0] != name {
				continue
			}
			value, err := strconv.ParseFloat(fields[1], 64)
			if _, found := values[name]; found || err != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("invalid resource metric %s", name)
			}
			values[name] = value
		}
	}
	for _, name := range resourceMetrics {
		if _, found := values[name]; !found {
			return nil, fmt.Errorf("missing resource metric %s", name)
		}
	}
	return values, nil
}

func (harness *harness) resourceSnapshot() (resourceSample, error) {
	status, body, err := harness.fetch("/metrics")
	if err != nil || status != http.StatusOK {
		return resourceSample{}, fmt.Errorf("resource scrape HTTP %d: %v", status, err)
	}
	values, err := parseResources(string(body))
	return resourceSample{Seconds: time.Since(harness.started).Seconds(), Values: values}, err
}

func resourceDistribution(values []float64) map[string]float64 {
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	result := map[string]float64{"count": float64(len(values))}
	for name, quantile := range map[string]float64{"p50": 0.5, "p95": 0.95, "p99": 0.99, "max": 1} {
		result[name] = ordered[int(math.Ceil(float64(len(ordered))*quantile))-1]
	}
	return result
}

func summarizeResources(samples []resourceSample) (map[string]any, error) {
	var steady []resourceSample
	for _, sample := range samples {
		if sample.Seconds >= 20 && sample.Seconds <= 45 {
			steady = append(steady, sample)
		}
	}
	if len(steady) < 20 {
		return nil, errors.New("insufficient steady resource samples")
	}
	first, last := steady[0], steady[len(steady)-1]
	duration := last.Seconds - first.Seconds
	if duration <= 0 {
		return nil, errors.New("resource clock did not advance")
	}
	result := map[string]any{"steady_seconds": duration, "samples": len(steady)}
	for metric, label := range map[string]string{
		"process_cpu_seconds_total": "cpu_cores", "go_memstats_alloc_bytes_total": "allocated_bytes_per_second",
		"go_memstats_mallocs_total": "allocations_per_second", "process_network_receive_bytes_total": "network_receive_bytes_per_second",
		"process_network_transmit_bytes_total": "network_transmit_bytes_per_second",
	} {
		for index := 1; index < len(steady); index++ {
			if steady[index].Values[metric] < steady[index-1].Values[metric] {
				return nil, fmt.Errorf("resource counter reset: %s", metric)
			}
		}
		result[label] = (last.Values[metric] - first.Values[metric]) / duration
	}
	for _, metric := range []string{"process_resident_memory_bytes", "go_memstats_heap_alloc_bytes", "go_memstats_heap_inuse_bytes", "go_memstats_heap_objects", "go_goroutines", "process_open_fds"} {
		var values []float64
		for _, sample := range steady {
			values = append(values, sample.Values[metric])
		}
		result[metric] = resourceDistribution(values)
	}
	var requests []float64
	for _, sample := range steady {
		if sample.HTTPSeconds <= 0 || sample.HTTPBodyBytes <= 0 {
			return nil, errors.New("missing steady HLS response probe")
		}
		requests = append(requests, sample.HTTPSeconds)
	}
	result["hls_get_seconds"] = resourceDistribution(requests)
	return result, nil
}

func (harness *harness) resourceBaseline() (result error) {
	groups := 1
	if configured := os.Getenv("STREAMD_E2E_RESOURCE_GROUPS"); configured != "" {
		parsed, err := strconv.Atoi(configured)
		if err != nil || (parsed != 1 && parsed != 2 && parsed != 4) {
			return errors.New("STREAMD_E2E_RESOURCE_GROUPS must be 1, 2 or 4")
		}
		groups = parsed
	}
	harness.recordingApplications("immediate", 4, 3)
	container, err := harness.startFilesystemContainerWithLimits(64*1024*1024, 64*1024*1024)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, container.close(harness)) }()
	defer harness.cleanup()
	harness.results["resource_conditions"] = map[string]any{"publishers": 2, "players": groups * 6, "container_cpus": 1, "container_memory_bytes": 512 * 1024 * 1024, "fixture": "bframes.flv", "media_seconds": 60, "play_seconds": 50, "steady_window_seconds": []int{20, 45}, "force_gc": false}
	initial, err := harness.resourceSnapshot()
	if err != nil {
		return err
	}
	harness.results["resource_initial"] = initial
	ctx, cancel := context.WithCancel(harness.ctx)
	done := make(chan error, 1)
	var samples []resourceSample
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-ticker.C:
				sample, err := harness.resourceSnapshot()
				if err == nil && sample.Seconds >= 15 && sample.Seconds <= 50 {
					playlist, failure := harness.playlist("flv")
					if failure == nil {
						location, failure := segmentURL(harness.origin, "flv", playlist)
						if failure == nil {
							started := time.Now()
							status, body, failure := harness.fetch(location)
							sample.HTTPSeconds, sample.HTTPBodyBytes = time.Since(started).Seconds(), len(body)
							if failure != nil || status != http.StatusOK || len(body) == 0 || len(body)%188 != 0 {
								err = fmt.Errorf("resource HLS probe HTTP %d: %v", status, failure)
							}
						} else {
							err = failure
						}
					} else {
						err = failure
					}
				}
				if err != nil {
					done <- err
					return
				}
				samples = append(samples, sample)
			}
		}
	}()
	finishSampling := sync.OnceValue(func() error {
		cancel()
		failure := <-done
		harness.results["resource_samples"] = samples
		if failure == nil {
			harness.results["resource_summary"], failure = summarizeResources(samples)
		}
		return failure
	})
	defer func() { result = errors.Join(result, finishSampling()) }()
	if err := harness.sleep(5 * time.Second); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
	}
	launched := time.Now()
	players := map[string]*process{}
	for group := 0; group < groups; group++ {
		for _, app := range []string{"flv", "ts"} {
			clients, err := harness.players(app, fmt.Sprintf("%d-%s-", group, app), 50)
			if err != nil {
				return err
			}
			maps.Copy(players, clients)
		}
	}
	watchStarted := time.Now()
	watch := harness.watchPlayback(players)
	defer func() { result = errors.Join(result, watch.finish(harness)) }()
	if err := harness.finishPlayers(players, 1125); err != nil {
		return err
	}
	if err := watch.finish(harness); err != nil {
		return err
	}
	firstFrames := map[string]float64{}
	for _, point := range watch.trace {
		name := point["player"].(string)
		if point["frames"].(int) > 0 && firstFrames[name] == 0 {
			firstFrames[name] = watchStarted.Sub(launched).Seconds() + point["elapsed"].(float64)
		}
	}
	if len(firstFrames) != len(players) {
		return errors.New("missing first decoded frame observations")
	}
	harness.results["first_frame_seconds_since_batch_launch"] = firstFrames
	if err := harness.finishPublishers(publishers, "publish-", 15*time.Second); err != nil {
		return err
	}
	if err := harness.sleep(10 * time.Second); err != nil {
		return err
	}
	retired, err := harness.resourceSnapshot()
	if err != nil {
		return err
	}
	harness.results["resource_retired"] = retired
	if retired.Values["go_goroutines"] > initial.Values["go_goroutines"]+4 || retired.Values["process_open_fds"] > initial.Values["process_open_fds"]+2 {
		return errors.New("media goroutines or file descriptors did not retire")
	}
	report, err := harness.configuration()
	if err != nil || len(report.Components) != 0 {
		return fmt.Errorf("media components did not retire: %v", err)
	}
	if err := finishSampling(); err != nil {
		return err
	}
	if err := container.collect(harness); err != nil {
		return err
	}
	for _, location := range []string{"recordings/fault/flv", "recordings/healthy/ts"} {
		if err := harness.recordings(location, 10, true); err != nil {
			return err
		}
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var event struct{ Level string }
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return err
		}
		if event.Level == "WARN" || event.Level == "ERROR" {
			return fmt.Errorf("unexpected resource-test service event: %s", line)
		}
	}
	return harness.checkLogs("publish-*.log", "play-*.log")
}
