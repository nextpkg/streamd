package main

import (
	"errors"
	"fmt"
)

func summarizeStabilityResources(samples []resourceSample, seconds int) (map[string]any, error) {
	if seconds < 60 || len(samples) < seconds*9/10 || samples[0].Seconds > 1 || samples[len(samples)-1].Seconds < float64(seconds) {
		return nil, errors.New("resource samples do not cover the entire stability window")
	}
	counters := map[string]string{"process_cpu_seconds_total": "cpu_cores", "go_memstats_alloc_bytes_total": "allocated_bytes_per_second", "go_memstats_mallocs_total": "allocations_per_second", "process_network_receive_bytes_total": "network_receive_bytes_per_second", "process_network_transmit_bytes_total": "network_transmit_bytes_per_second"}
	rates := map[string][]float64{}
	gauges := map[string][]float64{}
	latencies := []float64{}
	firstWindow, lastWindow := map[string][]float64{}, map[string][]float64{}
	for index, sample := range samples {
		if sample.HTTPSeconds <= 0 || sample.HTTPBodyBytes <= 0 {
			return nil, errors.New("missing full-window HLS response measurement")
		}
		latencies = append(latencies, sample.HTTPSeconds)
		for _, name := range resourceMetrics {
			value, present := sample.Values[name]
			if !present || value < 0 {
				return nil, fmt.Errorf("invalid stability metric %s", name)
			}
			gauges[name] = append(gauges[name], value)
			if sample.Seconds >= 10 && sample.Seconds <= 30 {
				firstWindow[name] = append(firstWindow[name], value)
			}
			if sample.Seconds >= float64(seconds-20) {
				lastWindow[name] = append(lastWindow[name], value)
			}
		}
		if index == 0 {
			continue
		}
		previous := samples[index-1]
		interval := sample.Seconds - previous.Seconds
		if interval <= 0 || interval > 5 {
			return nil, errors.New("resource sampling clock or coverage gap invalid")
		}
		for name, label := range counters {
			delta := sample.Values[name] - previous.Values[name]
			if delta < 0 {
				return nil, fmt.Errorf("resource counter reset: %s", name)
			}
			rates[label] = append(rates[label], delta/interval)
		}
	}
	result := map[string]any{"samples": len(samples), "observed_seconds": samples[len(samples)-1].Seconds - samples[0].Seconds, "hls_get_seconds": resourceDistribution(latencies), "warm_comparison_window": []int{10, 30}, "tail_comparison_window": []int{seconds - 20, seconds}}
	for label, values := range rates {
		result[label] = resourceDistribution(values)
	}
	for _, name := range []string{"process_resident_memory_bytes", "go_memstats_heap_alloc_bytes", "go_memstats_heap_inuse_bytes", "go_memstats_heap_objects", "go_goroutines", "process_open_fds"} {
		result[name] = resourceDistribution(gauges[name])
	}
	growth := map[string]any{}
	for name, allowance := range map[string]float64{"process_resident_memory_bytes": 64 * 1024 * 1024, "go_memstats_heap_alloc_bytes": 32 * 1024 * 1024, "go_goroutines": 4, "process_open_fds": 2} {
		if len(firstWindow[name]) < 15 || len(lastWindow[name]) < 15 {
			return nil, errors.New("insufficient warm/tail resource samples")
		}
		before := resourceDistribution(firstWindow[name])["p50"]
		after := resourceDistribution(lastWindow[name])["p50"]
		if after > before+allowance {
			return nil, fmt.Errorf("unbounded stability resource growth for %s: warm=%g tail=%g allowance=%g", name, before, after, allowance)
		}
		growth[name] = map[string]float64{"warm_p50": before, "tail_p50": after, "allowed_growth": allowance}
	}
	result["growth_checks"] = growth
	return result, nil
}
