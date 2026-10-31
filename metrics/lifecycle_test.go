package metrics

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestLifecycleDeltasSurviveConcurrentClusterChanges(t *testing.T) {
	registry := NewRegistry("initial", 1)
	registry.Average("live", "sample", ReadCost, 1) // Fill the stream-label budget.
	names := []string{Channels, Playback, Recording}
	var workers sync.WaitGroup
	for range 128 {
		workers.Go(func() {
			for _, name := range names {
				registry.AddGlobal(name, 1)
			}
		})
		workers.Go(func() { registry.SetCluster("switching") })
	}
	workers.Wait()
	registry.SetCluster("final")
	check := func(want int) {
		t.Helper()
		body := metricText(registry)
		for _, name := range names {
			expected := fmt.Sprintf("streamd_%s{app=\"\",channel=\"\",cluster=\"final\"} %d\n", name, want)
			if !strings.Contains(body, expected) {
				t.Errorf("missing lifecycle gauge: %s", expected)
			}
		}
		if strings.Contains(body, `cluster="initial"`) || strings.Contains(body, `cluster="switching"`) {
			t.Error("retired cluster label reappeared")
		}
	}
	check(128)
	for range 128 {
		workers.Go(func() {
			for _, name := range names {
				registry.AddGlobal(name, -1)
			}
		})
	}
	workers.Wait()
	check(0)
}
