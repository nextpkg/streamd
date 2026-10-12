package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestMediaMetricEvidenceRejectsMissingStaleAndInvalidSamples(test *testing.T) {
	body := "streamd_metric_updates_rejected_total 0\n"
	for metric, value := range map[string]int{"channels": 2, "playback": 8, "recording": 2} {
		body += fmt.Sprintf("streamd_%s{app=\"\",channel=\"\",cluster=\"current\"} %d\n", metric, value)
	}
	for _, app := range []string{"flv", "ts"} {
		for _, metric := range []string{"recording_bytes_total", "recording_seconds_total", "read_microseconds_count", "write_microseconds_count", "channels_bits_per_second_count", "playback_bits_per_second_count"} {
			body += fmt.Sprintf("streamd_%s{app=\"%s\",channel=\"sample\",cluster=\"current\"} 1\n", metric, app)
		}
	}
	if _, err := liveMetricValues(body, "current", true); err != nil {
		test.Fatal(err)
	}
	for _, invalid := range []string{"", body + body, strings.Replace(body, `cluster="current"`, `cluster="old"`, 1), strings.Replace(body, "total 0", "total 1", 1), strings.Replace(body, "} 2", "} 0", 1), strings.Replace(body, "} 1", "} NaN", 1), strings.Replace(body, "} 1", "} +Inf", 1), strings.Replace(body, "} 1", "} -1", 1), strings.Replace(body, "} 1", "} 1 extra", 1), strings.ReplaceAll(body, "streamd_recording_seconds_total", "missing_recording_seconds_total")} {
		if _, err := liveMetricValues(invalid, "current", true); err == nil {
			test.Fatal("incomplete or corrupt metric evidence accepted")
		}
	}
	if _, err := liveMetricValues(body, "current", false); err == nil {
		test.Fatal("active resources counted as retired")
	}
}
