package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nextpkg/streamd/config"
)

func liveMetricValues(body, cluster string, active bool) (map[string]float64, error) {
	values := map[string]float64{}
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "streamd_") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, errors.New("malformed metric sample")
		}
		name := fields[0]
		if strings.Contains(name, "cluster=") && !strings.Contains(name, `cluster="`+cluster+`"`) {
			return nil, errors.New("unexpected metric cluster")
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if _, exists := values[name]; exists || err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return nil, errors.New("duplicate or invalid metric value")
		}
		values[name] = value
	}
	counts := map[string]float64{"channels": 0, "playback": 0, "recording": 0}
	if active {
		counts = map[string]float64{"channels": 2, "playback": 8, "recording": 2}
	}
	for metric, expected := range counts {
		name := fmt.Sprintf(`streamd_%s{app="",channel="",cluster="%s"}`, metric, cluster)
		if value, found := values[name]; !found || value != expected {
			return nil, fmt.Errorf("%s: got %v (present=%v), want %v", metric, value, found, expected)
		}
	}
	if rejected, found := values["streamd_metric_updates_rejected_total"]; !found || rejected != 0 {
		return nil, errors.New("ordinary media metrics were rejected")
	}
	for _, app := range []string{"flv", "ts"} {
		for _, metric := range []string{"recording_bytes_total", "recording_seconds_total", "read_microseconds_count", "write_microseconds_count", "channels_bits_per_second_count", "playback_bits_per_second_count"} {
			name := fmt.Sprintf(`streamd_%s{app="%s",channel="sample",cluster="%s"}`, metric, app, cluster)
			if value, found := values[name]; !found || value <= 0 {
				return nil, fmt.Errorf("missing media observation %s", name)
			}
		}
	}
	return values, nil
}

func (harness *harness) metricSnapshot(label, cluster string, active bool) error {
	var body []byte
	var values map[string]float64
	if err := harness.until("metric state "+label, 8*time.Second, func() (bool, error) {
		status, data, err := harness.fetch("/metrics")
		if err != nil || status != http.StatusOK {
			return false, fmt.Errorf("metrics HTTP %d: %v", status, err)
		}
		body = data
		values, err = liveMetricValues(string(body), cluster, active)
		return err == nil, err
	}); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(harness.evidence, "metrics-"+label+".txt"), body, 0600); err != nil {
		return err
	}
	harness.results["metrics_"+label] = values
	return nil
}

func (harness *harness) metricsMedia() error {
	harness.recordingApplications("immediate", 4, 3)
	harness.settings.Metrics = &config.Metrics{Cluster: "initial"}
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
		group, err := harness.players(app, app+"-", 50)
		if err != nil {
			return err
		}
		for name, child := range group {
			players[name] = child
		}
	}
	watch := harness.watchPlayback(players)
	defer watch.finish(harness)
	if err := harness.metricSnapshot("initial", "initial", true); err != nil {
		return err
	}
	for index, cluster := range []string{"updated", "final"} {
		if err := harness.waitAt(14 + index*14); err != nil {
			return err
		}
		harness.settings.Metrics.Cluster = cluster
		if err := harness.commit("cluster-"+cluster, nil); err != nil {
			return err
		}
		if err := harness.metricSnapshot(cluster, cluster, true); err != nil {
			return err
		}
	}
	if err := harness.finishPlayers(players, 50*25*9/10); err != nil {
		return err
	}
	if err := watch.finish(harness); err != nil {
		return err
	}
	if err := harness.finishPublishers(publishers, "publish-", 15*time.Second); err != nil {
		return err
	}
	if err := harness.metricSnapshot("retired", "final", false); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		if err := harness.recordings(filepath.Join("recordings", app), 10, true); err != nil {
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
			return fmt.Errorf("unexpected service event: %s", line)
		}
	}
	harness.results["metrics_live_verified"] = true
	return harness.checkLogs("publish-*.log", "play-*.log")
}
