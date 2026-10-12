package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (container *releaseContainer) verifyCLI(harness *harness) error {
	for route, expected := range map[string]string{"/healthz": "ok\n", "/readyz": "ready configuration=1\n"} {
		status, body, err := harness.fetch(route)
		if err != nil || status != http.StatusOK || string(body) != expected {
			return fmt.Errorf("release diagnostic %s: HTTP %d body=%q error=%v", route, status, body, err)
		}
	}
	probe := func(label, filename, expected string, arguments ...string) (result error) {
		ctx, cancel := context.WithTimeout(harness.ctx, 10*time.Second)
		defer cancel()
		name := container.runner.name + "-cli-" + label
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			if _, err := container.runner.command(cleanup, "container", "inspect", name); err == nil {
				_, err = container.runner.command(cleanup, "rm", "--force", "--volumes", name)
				result = errors.Join(result, err)
			}
		}()
		command := []string{"run", "--rm", "--name", name, "--label", "io.streamd.test-run=" + container.runner.name,
			"--network", "container:" + container.runner.name, "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
			"--mount", "type=bind,source=" + harness.evidence + ",target=" + harness.evidence + ",readonly", container.image, "-config", filename}
		command = append(command, arguments...)
		body, err := container.runner.command(ctx, command...)
		if failure := os.WriteFile(filepath.Join(harness.evidence, "cli-"+label+".log"), body, 0600); failure != nil {
			return failure
		}
		if ctx.Err() != nil || !strings.Contains(string(body), expected) {
			return fmt.Errorf("CLI %s did not report expected result: %v: %s", label, err, body)
		}
		if (label == "check") != (err == nil) {
			return fmt.Errorf("CLI %s unexpected exit: %v", label, err)
		}
		return nil
	}
	current := filepath.Join(harness.evidence, "config.json")
	if err := probe("check", current, "configuration valid", "-check-config"); err != nil {
		return err
	}
	if err := probe("rtmp-conflict", current, "bind: address already in use"); err != nil {
		return err
	}
	configuration := harness.settings
	port, err := freePort()
	if err != nil {
		return err
	}
	configuration.RTMP.Addr = fmt.Sprintf("0.0.0.0:%d", port)
	filename := filepath.Join(harness.evidence, "cli-http-conflict.json")
	if err := writeJSON(filename, configuration); err != nil {
		return err
	}
	if err := os.Chmod(filename, 0644); err != nil {
		return err
	}
	if err := probe("http-conflict", filename, "bind: address already in use"); err != nil {
		return err
	}
	filename = filepath.Join(harness.evidence, "cli-invalid.json")
	if err := os.WriteFile(filename, []byte("{\"unexpected_field\":true}\n"), 0644); err != nil {
		return err
	}
	if err := probe("invalid", filename, "unknown field", "-check-config"); err != nil {
		return err
	}
	status, _, err := harness.fetch("/readyz")
	if err != nil || status != http.StatusOK || !harness.server.alive() {
		return errors.New("CLI probes disturbed the existing service")
	}
	harness.results["release_cli_verified"] = map[string]bool{"version_matches_image": true, "health_and_readiness": true, "check_config_does_not_bind": true, "invalid_configuration_rejected": true, "occupied_rtmp_rejected": true, "occupied_http_rejected": true, "existing_service_healthy": true}
	return nil
}
