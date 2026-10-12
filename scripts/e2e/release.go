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

type releaseContainer struct {
	runner    *objectContainer
	image     string
	attempted bool
	reloads   int
	volume    string
}

func (harness *harness) releaseMedia() (result error) {
	if harness.results["race_build"] == true {
		return errors.New("release image must use its actual pure-Go production binary")
	}
	image := os.Getenv("STREAMD_E2E_RELEASE_IMAGE")
	if !strings.HasPrefix(image, "sha256:") || len(image) != 71 {
		return errors.New("STREAMD_E2E_RELEASE_IMAGE must be a local immutable image ID")
	}
	runtimeContext := os.Getenv("STREAMD_DOCKER_CONTEXT")
	if runtimeContext == "" {
		runtimeContext = "colima-native"
	}
	container := &releaseContainer{image: image, runner: &objectContainer{context: runtimeContext, name: "streamd-" + filepath.Base(harness.evidence)}}
	harness.release = container
	defer func() { result = errors.Join(result, container.close(harness)) }()
	defer func() {
		if result != nil && container.attempted {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, err := container.runner.command(ctx, "cp", container.runner.name+":/data/.", filepath.Join(harness.evidence, "recordings"))
			harness.results["release_failure_recordings_preserved"] = err == nil
			result = errors.Join(result, err)
		}
	}()
	defer harness.cleanup()
	probe, cancel := context.WithTimeout(harness.ctx, 10*time.Second)
	defer cancel()
	body, err := container.runner.command(probe, "image", "inspect", image)
	if err != nil {
		return err
	}
	var images []struct {
		ID     string `json:"Id"`
		Config struct {
			User       string
			Entrypoint []string
			Labels     map[string]string
		}
	}
	if err := json.Unmarshal(body, &images); err != nil {
		return err
	}
	if len(images) != 1 || images[0].ID != image || images[0].Config.User != "65532:65532" || len(images[0].Config.Entrypoint) != 1 || images[0].Config.Entrypoint[0] != "/streamd" {
		return errors.New("unexpected release image identity or runtime user")
	}
	version, err := container.runner.command(probe, "run", "--rm", "--network", "none", "--read-only", "--cap-drop", "ALL", image, "-version")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(version)) != images[0].Config.Labels["org.opencontainers.image.version"] {
		return errors.New("release binary version differs from image label")
	}
	harness.results["release_image"] = map[string]any{"id": image, "user": images[0].Config.User, "version": strings.TrimSpace(string(version)), "read_only": true}
	media := harness.metricsMedia
	if harness.results["scenario"] == "stability" {
		media = harness.stabilityMedia
	}
	if err := media(); err != nil {
		return err
	}
	body, err = container.runner.command(harness.ctx, "inspect", container.runner.name)
	if err != nil {
		return err
	}
	var states []struct {
		State struct {
			ExitCode  int
			Running   bool
			OOMKilled bool
		}
	}
	if err := json.Unmarshal(body, &states); err != nil {
		return err
	}
	if len(states) != 1 || states[0].State.Running || states[0].State.OOMKilled || states[0].State.ExitCode != 0 {
		return errors.New("release container did not exit cleanly")
	}
	harness.results["release_container_exit"] = states[0].State
	return nil
}

func (container *releaseContainer) start(harness *harness) error {
	_, httpPort, err := net.SplitHostPort(harness.settings.HTTP.Addr)
	if err != nil {
		return err
	}
	harness.settings.HTTP.Addr = net.JoinHostPort("0.0.0.0", httpPort)
	harness.settings.RTMP.Addr = fmt.Sprintf("0.0.0.0:%d", harness.rtmpPort)
	for index := range harness.settings.Application {
		harness.settings.Application[index].Recorder.SaveTo = "/data"
	}
	if err := os.Chmod(harness.evidence, 0755); err != nil {
		return err
	}
	directory := filepath.Join(harness.evidence, "recordings")
	if err := os.MkdirAll(directory, 0755); err != nil {
		return err
	}
	filename := filepath.Join(harness.evidence, "config.json")
	if err := writeJSON(filename, harness.settings); err != nil {
		return err
	}
	if err := os.Chmod(filename, 0644); err != nil {
		return err
	}
	container.volume = container.runner.name + "-data"
	ctx, cancel := context.WithTimeout(harness.ctx, 10*time.Second)
	defer cancel()
	container.attempted = true
	if _, err := container.runner.command(ctx, "volume", "create", "--label", "io.streamd.test-run="+container.runner.name, container.volume); err != nil {
		return err
	}
	arguments := []string{"docker", "--context", container.runner.context, "run", "--name", container.runner.name,
		"--pull", "never", "--label", "io.streamd.test-run=" + container.runner.name,
		"--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--memory", "512m", "--pids-limit", "128",
		"--mount", "type=bind,source=" + harness.evidence + ",target=" + harness.evidence + ",readonly",
		"--mount", "type=volume,source=" + container.volume + ",target=/data",
		"--publish", fmt.Sprintf("127.0.0.1:%d:%d", harness.rtmpPort, harness.rtmpPort),
		"--publish", "127.0.0.1:" + httpPort + ":" + httpPort}
	if harness.results["scenario"] == "stability" {
		arguments = append(arguments, "--cpus", "1")
	}
	arguments = append(arguments, container.image, "-config", filename)
	harness.server, err = harness.launch("server", arguments...)
	if err != nil {
		return err
	}
	if err := harness.until("release container readiness", 15*time.Second, func() (bool, error) {
		status, _, err := harness.fetch("/readyz")
		return status == http.StatusOK, err
	}); err != nil {
		return err
	}
	harness.started = time.Now()
	return nil
}

func (container *releaseContainer) collect(harness *harness) error {
	if err := harness.until("release recordings naturally closed", 5*time.Second, func() (bool, error) {
		report, err := harness.configuration()
		if err != nil {
			return false, err
		}
		for _, component := range report.Components {
			if component.Component == "recording" {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	directory := filepath.Join(harness.evidence, "recordings")
	budget := 10 * time.Second
	if harness.results["scenario"] == "stability" {
		budget = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(harness.ctx, budget)
	defer cancel()
	_, err := container.runner.command(ctx, "cp", container.runner.name+":/data/.", directory)
	return err
}

func (container *releaseContainer) reload(harness *harness) error {
	ctx, cancel := context.WithTimeout(harness.ctx, 5*time.Second)
	defer cancel()
	if _, err := container.runner.command(ctx, "kill", "--signal", "HUP", container.runner.name); err != nil {
		return err
	}
	container.reloads++
	harness.results["release_reload"] = map[string]any{"method": "atomic replacement followed by SIGHUP", "signals_sent": container.reloads}
	return nil
}

func (container *releaseContainer) close(harness *harness) (result error) {
	if !container.attempted {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	defer func() {
		if container.volume != "" {
			body, err := container.runner.command(ctx, "volume", "inspect", "--format", "{{index .Labels \"io.streamd.test-run\"}}", container.volume)
			if err == nil && strings.TrimSpace(string(body)) == container.runner.name {
				_, err = container.runner.command(ctx, "volume", "rm", container.volume)
			} else if err == nil {
				err = errors.New("refusing to remove an unowned volume")
			}
			result = errors.Join(result, err)
		}
		harness.results["release_container_cleanup"] = map[string]any{"complete": result == nil, "container": container.runner.name, "volume": container.volume, "image_retained": container.image}
	}()
	body, err := container.runner.command(ctx, "inspect", "--format", "{{index .Config.Labels \"io.streamd.test-run\"}}", container.runner.name)
	if err != nil {
		if strings.Contains(string(body), "No such object") {
			return nil
		}
		return err
	}
	if strings.TrimSpace(string(body)) != container.runner.name {
		return errors.New("refusing to remove an unowned container")
	}
	_, err = container.runner.command(ctx, "rm", "--force", "--volumes", container.runner.name)
	return err
}
