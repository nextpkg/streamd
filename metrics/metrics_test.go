package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricSemantics(test *testing.T) {
	registry := NewRegistry("local", 10)
	registry.Gauge("live", "stream", Playback, 3)
	registry.Gauge("live", "stream", Playback, 2)
	registry.Counter("live", "stream", RecordingSize, 5)
	registry.Counter("live", "stream", RecordingSize, 7)
	registry.Average("live", "stream", ReadCost, 10)
	registry.Average("live", "stream", ReadCost, 20)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	for _, expected := range []string{
		`streamd_playback{app="live",channel="stream",cluster="local"} 2`,
		`streamd_recording_bytes_total{app="live",channel="stream",cluster="local"} 12`,
		`streamd_read_microseconds_sum{app="live",channel="stream",cluster="local"} 30`,
		`streamd_read_microseconds_count{app="live",channel="stream",cluster="local"} 2`,
	} {
		if !strings.Contains(response.Body.String(), expected) {
			test.Errorf("missing metric %s", expected)
		}
	}
}

func TestMetricCardinalityLimit(test *testing.T) {
	registry := NewRegistry("local", 1)
	registry.Gauge("live", "first", Playback, 1)
	registry.Gauge("live", "second", Playback, 1)
	registry.Gauge("live", "first", Playback, 2)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(response.Body.String(), `channel="second"`) {
		test.Fatal("cardinality limit was exceeded")
	}
	if !strings.Contains(response.Body.String(), "streamd_metric_updates_rejected_total 1") {
		test.Fatal("rejected updates were not observable")
	}
}

func TestReportLoopStops(test *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ReportLoop(ctx, func() {})
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		test.Fatal("metrics loop did not stop")
	}
}
