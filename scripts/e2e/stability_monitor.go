package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const stabilityPrometheusImage = "quay.io/prometheus/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996"

type stabilityMonitor struct {
	runner    *objectContainer
	service   string
	network   string
	origin    string
	pulled    bool
	created   bool
	connected bool
	attempted bool
}

func (harness *harness) startStabilityMonitor() (monitor *stabilityMonitor, result error) {
	monitor = &stabilityMonitor{runner: &objectContainer{context: harness.release.runner.context, name: harness.release.runner.name + "-prometheus"}, service: harness.release.runner.name, network: harness.release.runner.name + "-monitor"}
	defer func() {
		if result != nil {
			result = errors.Join(result, monitor.close(harness))
		}
	}()
	ctx, cancel := context.WithTimeout(harness.ctx, 2*time.Minute)
	defer cancel()
	if _, err := monitor.runner.command(ctx, "image", "inspect", stabilityPrometheusImage); err != nil {
		if _, err := monitor.runner.command(ctx, "pull", stabilityPrometheusImage); err != nil {
			return monitor, err
		}
		monitor.pulled = true
	}
	if _, err := monitor.runner.command(ctx, "network", "create", "--label", "io.streamd.test-run="+monitor.runner.name, monitor.network); err != nil {
		return monitor, err
	}
	monitor.created = true
	if _, err := monitor.runner.command(ctx, "network", "connect", monitor.network, monitor.service); err != nil {
		return monitor, err
	}
	monitor.connected = true
	_, servicePort, err := net.SplitHostPort(harness.settings.HTTP.Addr)
	if err != nil {
		return monitor, err
	}
	port, err := freePort()
	if err != nil {
		return monitor, err
	}
	monitor.origin = fmt.Sprintf("http://127.0.0.1:%d", port)
	configuration := fmt.Sprintf("global:\n  scrape_interval: 1s\n  scrape_timeout: 800ms\nscrape_configs:\n  - job_name: streamd\n    static_configs:\n      - targets: ['%s:%s']\n", monitor.service, servicePort)
	filename := filepath.Join(harness.evidence, "prometheus.yaml")
	if err := os.WriteFile(filename, []byte(configuration), 0644); err != nil {
		return monitor, err
	}
	monitor.attempted = true
	if _, err := monitor.runner.command(ctx, "run", "-d", "--name", monitor.runner.name, "--label", "io.streamd.test-run="+monitor.runner.name,
		"--pull", "never", "--network", monitor.network, "--read-only", "--user", "65534:65534", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--cpus", "0.5", "--memory", "512m", "--pids-limit", "128",
		"--tmpfs", "/prometheus:rw,nosuid,nodev,size=268435456,uid=65534,gid=65534,mode=0700", "--mount", "type=bind,source="+filename+",target=/etc/prometheus/prometheus.yml,readonly", "--publish", fmt.Sprintf("127.0.0.1:%d:9090", port), stabilityPrometheusImage,
		"--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.path=/prometheus", "--storage.tsdb.retention.time=3h", "--storage.tsdb.retention.size=128MB", "--web.listen-address=0.0.0.0:9090"); err != nil {
		return monitor, err
	}
	if err := harness.until("real Prometheus scrape target healthy", 20*time.Second, func() (bool, error) {
		return monitor.target(harness, false)
	}); err != nil {
		return monitor, err
	}
	harness.results["stability_prometheus"] = map[string]any{"image": stabilityPrometheusImage, "version": "3.5.0", "scrape_seconds": 1, "service_target": monitor.service + ":" + servicePort, "data": "owned tmpfs", "runtime_dependency": false}
	return monitor, nil
}

func (monitor *stabilityMonitor) target(harness *harness, save bool) (bool, error) {
	status, body, err := harness.fetch(monitor.origin + "/api/v1/targets")
	if err != nil || status != http.StatusOK {
		return false, fmt.Errorf("prometheus targets HTTP %d: %v", status, err)
	}
	var response struct {
		Status string
		Data   struct {
			ActiveTargets []struct {
				Labels    map[string]string
				Health    string
				LastError string
				ScrapeURL string
			}
		}
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return false, err
	}
	if response.Status != "success" || len(response.Data.ActiveTargets) != 1 {
		return false, errors.New("prometheus target inventory differs")
	}
	target := response.Data.ActiveTargets[0]
	if target.Health != "up" || target.LastError != "" || target.Labels["job"] != "streamd" || !strings.HasPrefix(target.ScrapeURL, "http://"+monitor.service+":") {
		return false, errors.New("real service scrape target is not healthy")
	}
	if save {
		if err := os.WriteFile(filepath.Join(harness.evidence, "prometheus-targets.json"), body, 0600); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (monitor *stabilityMonitor) query(harness *harness) error {
	if _, err := monitor.target(harness, true); err != nil {
		return err
	}
	parent := filepath.Join(harness.evidence, "queries")
	child, err := harness.launch("prometheus-query", "go", "run", "./scripts/loadtest", "query", "-address", monitor.origin, "-job", "streamd", "-out", parent)
	if err != nil {
		return err
	}
	if err := harness.finish("prometheus-query", child, 30*time.Second); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "prometheus-query.log"))
	if err != nil {
		return err
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	directory := lines[len(lines)-1]
	if filepath.Dir(directory) != parent {
		return errors.New("query command did not return its owned evidence directory")
	}
	body, err = os.ReadFile(filepath.Join(directory, "results.json"))
	if err != nil {
		return err
	}
	var report struct {
		Complete  bool
		Cancelled bool
		Metrics   []struct {
			Name    string
			Query   string
			Samples []struct {
				Labels map[string]string
				Value  float64
			}
		}
	}
	if err := json.Unmarshal(body, &report); err != nil {
		return err
	}
	if !report.Complete || report.Cancelled || len(report.Metrics) != 2 {
		return errors.New("real PromQL query result incomplete")
	}
	for index, name := range []string{"cpu_percent", "resident_memory_mib"} {
		metric := report.Metrics[index]
		if metric.Name != name || len(metric.Samples) != 1 || metric.Samples[0].Labels["job"] != "streamd" || !strings.HasPrefix(metric.Samples[0].Labels["instance"], monitor.service+":") || metric.Samples[0].Value <= 0 {
			return fmt.Errorf("invalid measured PromQL result %s", name)
		}
	}
	harness.results["stability_promql"] = report.Metrics
	return nil
}

func (monitor *stabilityMonitor) close(harness *harness) (result error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if monitor.attempted {
		if body, err := monitor.runner.command(ctx, "logs", monitor.runner.name); err == nil {
			result = errors.Join(result, os.WriteFile(filepath.Join(harness.evidence, "prometheus.log"), body, 0600))
		}
		if _, err := monitor.runner.command(ctx, "container", "inspect", monitor.runner.name); err == nil {
			_, err = monitor.runner.command(ctx, "rm", "--force", "--volumes", monitor.runner.name)
			result = errors.Join(result, err)
		}
	}
	if monitor.connected {
		_, err := monitor.runner.command(ctx, "network", "disconnect", "--force", monitor.network, monitor.service)
		result = errors.Join(result, err)
	}
	if monitor.created {
		_, err := monitor.runner.command(ctx, "network", "rm", monitor.network)
		result = errors.Join(result, err)
	}
	if monitor.pulled {
		_, err := monitor.runner.command(ctx, "image", "rm", stabilityPrometheusImage)
		result = errors.Join(result, err)
	}
	harness.results["stability_monitor_cleanup"] = map[string]any{"complete": result == nil, "container": monitor.runner.name, "network": monitor.network, "removed_owned_image": monitor.pulled && result == nil}
	return result
}
