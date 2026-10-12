package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func metricText(registry *Registry) string {
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	return response.Body.String()
}

func TestClusterChangeRetiresLabelsAndPreservesGlobalGauges(test *testing.T) {
	registry := NewRegistry("old", 2)
	registry.Gauge("", "", Channels, 2)
	registry.Counter("live", "stream", RecordingSize, 7)
	registry.Average("live", "stream", ReadCost, 3)
	registry.SetCluster("new")
	registry.Counter("live", "stream", RecordingSize, 5)
	registry.Average("live", "stream", ReadCost, 4)
	body := metricText(registry)
	for _, expected := range []string{
		`streamd_channels{app="",channel="",cluster="new"} 2`,
		`streamd_recording_bytes_total{app="live",channel="stream",cluster="new"} 5`,
		`streamd_read_microseconds_sum{app="live",channel="stream",cluster="new"} 4`,
		`streamd_read_microseconds_count{app="live",channel="stream",cluster="new"} 1`,
	} {
		if !strings.Contains(body, expected) {
			test.Errorf("missing metric after cluster change: %s", expected)
		}
	}
	if strings.Contains(body, `cluster="old"`) {
		test.Error("retired cluster still exported")
	}
}

func TestFullStreamCardinalityCannotSuppressGlobalGauges(test *testing.T) {
	registry := NewRegistry("local", 1)
	registry.Average("live", "first", ReadCost, 1)
	registry.Gauge("", "", Playback, 3)
	registry.Gauge("", "", Playback, 0)
	registry.Average("live", "second", ReadCost, 1)
	body := metricText(registry)
	if !strings.Contains(body, `streamd_playback{app="",channel="",cluster="local"} 0`) || !strings.Contains(body, "streamd_metric_updates_rejected_total 1\n") {
		test.Fatal("full stream label budget hid current global state or rejection count")
	}
	if strings.Contains(body, `channel="second"`) {
		test.Fatal("active stream label limit exceeded")
	}
}

func TestSameClusterPreservesMetricAccumulation(test *testing.T) {
	registry := NewRegistry("local", 1)
	registry.Counter("live", "stream", RecordingSize, 7)
	registry.Average("live", "stream", ReadCost, 3)
	registry.SetCluster("local")
	registry.Counter("live", "stream", RecordingSize, 5)
	registry.Average("live", "stream", ReadCost, 4)
	body := metricText(registry)
	if !strings.Contains(body, `streamd_recording_bytes_total{app="live",channel="stream",cluster="local"} 12`) || !strings.Contains(body, `streamd_read_microseconds_count{app="live",channel="stream",cluster="local"} 2`) {
		test.Fatal("unchanged cluster reset cumulative observations")
	}
}

func TestInactiveMetricSeriesRetireAcrossAllFamilies(test *testing.T) {
	registry := NewRegistry("local", 2)
	now := time.Unix(1000, 0)
	registry.now = func() time.Time { return now }
	registry.Gauge("", "", Channels, 1)
	registry.Gauge("live", "idle", Playback, 2)
	registry.Counter("live", "idle", RecordingSize, 7)
	registry.Average("live", "idle", ReadCost, 3)
	registry.Counter("live", "active", RecordingSize, 4)
	now = now.Add(4 * time.Minute)
	registry.Counter("live", "active", RecordingSize, 8)
	if !strings.Contains(metricText(registry), `channel="idle"`) {
		test.Fatal("idle series removed before retention interval")
	}
	now = now.Add(time.Minute)
	body := metricText(registry)
	if strings.Contains(body, `channel="idle"`) {
		test.Fatal("expired series still exported")
	}
	if !strings.Contains(body, `streamd_recording_bytes_total{app="live",channel="active",cluster="local"} 12`) || !strings.Contains(body, `streamd_channels{app="",channel="",cluster="local"} 1`) {
		test.Fatal("retirement removed active accumulation or global state")
	}
	registry.Counter("live", "new", RecordingSize, 9)
	if !strings.Contains(metricText(registry), `streamd_recording_bytes_total{app="live",channel="new",cluster="local"} 9`) {
		test.Fatal("retired label did not release its budget")
	}
}

func TestCardinalityPressureReclaimsIdleMetricsWithoutScraping(test *testing.T) {
	registry := NewRegistry("local", 1)
	now := time.Unix(1000, 0)
	registry.now = func() time.Time { return now }
	registry.Counter("live", "old", RecordingSize, 7)
	now = now.Add(5 * time.Minute)
	registry.Counter("live", "new", RecordingSize, 9)
	registry.Counter("live", "blocked", RecordingSize, 1)
	body := metricText(registry)
	if strings.Contains(body, `channel="old"`) || strings.Contains(body, `channel="blocked"`) || !strings.Contains(body, `streamd_recording_bytes_total{app="live",channel="new",cluster="local"} 9`) || !strings.Contains(body, "streamd_metric_updates_rejected_total 1\n") {
		test.Fatal("idle retirement or active label budget violated")
	}
}

func TestConcurrentMetricsCannotResurrectRetiredCluster(test *testing.T) {
	registry := NewRegistry("start", 2)
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Go(func() {
			for iteration := range 100 {
				switch worker {
				case 0:
					registry.SetCluster("first")
					registry.SetCluster("second")
				case 1:
					registry.Gauge("", "", Channels, 2)
					registry.Counter("live", "stream", RecordingSize, 1)
				case 2:
					registry.Average("live", "stream", ReadCost, float64(iteration))
				default:
					metricText(registry)
				}
			}
		})
	}
	workers.Wait()
	registry.SetCluster("final")
	registry.Counter("live", "stream", RecordingSize, 3)
	body := metricText(registry)
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, "cluster=") && !strings.Contains(line, `cluster="final"`) {
			test.Fatal("retired cluster resurrected after concurrent updates")
		}
	}
	if !strings.Contains(body, `streamd_channels{app="",channel="",cluster="final"} 2`) || !strings.Contains(body, `streamd_recording_bytes_total{app="live",channel="stream",cluster="final"} 3`) {
		test.Fatal("final cluster state missing")
	}
}
