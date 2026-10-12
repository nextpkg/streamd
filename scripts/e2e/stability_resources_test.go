package main

import (
	"testing"
)

func stabilityResourceFixture() []resourceSample {
	samples := make([]resourceSample, 61)
	for index := range samples {
		values := map[string]float64{}
		for _, name := range resourceMetrics {
			values[name] = float64(index + 1)
		}
		values["go_goroutines"], values["process_open_fds"] = 50, 20
		samples[index] = resourceSample{Seconds: float64(index), Values: values, HTTPSeconds: 0.01, HTTPBodyBytes: 188}
	}
	return samples
}

func TestStabilityResourcesRequireFullCoverageAndMonotonicCounters(test *testing.T) {
	if _, err := summarizeStabilityResources(stabilityResourceFixture(), 60); err != nil {
		test.Fatal(err)
	}
	for _, mode := range []string{"prefix", "tail", "gap", "counter", "probe", "memory", "goroutines", "descriptors"} {
		test.Run(mode, func(test *testing.T) {
			samples := stabilityResourceFixture()
			switch mode {
			case "prefix":
				samples = samples[2:]
			case "tail":
				samples = samples[:60]
			case "gap":
				samples = append(samples[:31], samples[37:]...)
			case "counter":
				samples[20].Values["process_cpu_seconds_total"] = 0
			case "probe":
				samples[20].HTTPSeconds = 0
			default:
				name := map[string]string{"memory": "process_resident_memory_bytes", "goroutines": "go_goroutines", "descriptors": "process_open_fds"}[mode]
				for index := 40; index < len(samples); index++ {
					samples[index].Values[name] += 128 * 1024 * 1024
				}
			}
			if _, err := summarizeStabilityResources(samples, 60); err == nil {
				test.Fatal("accepted incomplete or unstable resource evidence")
			}
		})
	}
}
