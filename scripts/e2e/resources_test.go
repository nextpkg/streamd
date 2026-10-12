package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestResourceMetricsRejectMissingInvalidAndDuplicate(test *testing.T) {
	var valid strings.Builder
	for _, name := range resourceMetrics {
		fmt.Fprintf(&valid, "%s 1\n", name)
	}
	if _, err := parseResources(valid.String()); err != nil {
		test.Fatal(err)
	}
	for _, value := range []string{"NaN", "+Inf", "-1", "invalid"} {
		candidate := strings.Replace(valid.String(), "process_cpu_seconds_total 1", "process_cpu_seconds_total "+value, 1)
		if _, err := parseResources(candidate); err == nil {
			test.Errorf("accepted %s", value)
		}
	}
	for _, candidate := range []string{strings.Replace(valid.String(), "process_resident_memory_bytes 1\n", "", 1), valid.String() + "process_open_fds 2\n"} {
		if _, err := parseResources(candidate); err == nil {
			test.Error("accepted missing or duplicated resource sample")
		}
	}
}

func TestResourceSummaryRejectsResetAndIncompleteMeasurements(test *testing.T) {
	makeSamples := func() []resourceSample {
		var samples []resourceSample
		for second := 20; second <= 45; second++ {
			values := map[string]float64{}
			for _, name := range resourceMetrics {
				values[name] = float64(second * 2)
			}
			samples = append(samples, resourceSample{Seconds: float64(second), Values: values, HTTPSeconds: float64(second) / 1000, HTTPBodyBytes: 188})
		}
		return samples
	}
	summary, err := summarizeResources(makeSamples())
	if err != nil || summary["cpu_cores"] != float64(2) || summary["hls_get_seconds"].(map[string]float64)["p99"] != 0.045 {
		test.Fatalf("unexpected resource summary: %v: %v", summary, err)
	}
	for _, mutation := range []func([]resourceSample) []resourceSample{
		func(samples []resourceSample) []resourceSample { return samples[:10] },
		func(samples []resourceSample) []resourceSample {
			samples[10].Values["process_cpu_seconds_total"] = 0
			return samples
		},
		func(samples []resourceSample) []resourceSample {
			samples[10].HTTPBodyBytes = 0
			return samples
		},
	} {
		if _, err := summarizeResources(mutation(makeSamples())); err == nil {
			test.Error("accepted incomplete or reset resource measurements")
		}
	}
}
