package metrics

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestProcessResourceMetrics(test *testing.T) {
	registry := NewRegistry("resources", 1)
	registry.Gauge("live", "first", Playback, 1)
	registry.Gauge("live", "overflow", Playback, 1)
	previousCPU := float64(-1)
	for _, cluster := range []string{"resources", "reloaded"} {
		registry.SetCluster(cluster)
		response := httptest.NewRecorder()
		registry.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if response.Code != http.StatusOK {
			test.Fatalf("metrics HTTP status %d", response.Code)
		}
		values := map[string]float64{}
		for _, line := range strings.Split(response.Body.String(), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 2 || !strings.HasPrefix(fields[0], "process_") {
				continue
			}
			value, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				test.Fatal(err)
			}
			values[fields[0]] = value
		}
		for _, name := range []string{"process_cpu_seconds_total", "process_start_time_seconds", "process_open_fds", "process_max_fds"} {
			value, exists := values[name]
			if !exists || value < 0 {
				test.Errorf("missing or invalid process metric %s", name)
			}
		}
		if values["process_cpu_seconds_total"] < previousCPU {
			test.Error("configuration reload reset process CPU time")
		}
		previousCPU = values["process_cpu_seconds_total"]
		if runtime.GOOS == "linux" {
			for _, name := range []string{"process_resident_memory_bytes", "process_virtual_memory_bytes"} {
				if values[name] <= 0 {
					test.Errorf("missing Linux process memory metric %s", name)
				}
			}
		}
	}
}
