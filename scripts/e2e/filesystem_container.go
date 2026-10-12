package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const filesystemImage = "docker.io/library/busybox@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e"

type filesystemContainer struct {
	runner *objectContainer
	pulled bool
}

func (container *filesystemContainer) close(harness *harness) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var result error
	if _, err := container.runner.command(ctx, "container", "inspect", container.runner.name); err == nil {
		_, err = container.runner.command(ctx, "rm", "--force", "--volumes", container.runner.name)
		result = errors.Join(result, err)
	}
	if container.pulled {
		_, err := container.runner.command(ctx, "image", "rm", filesystemImage)
		result = errors.Join(result, err)
	}
	harness.results["filesystem_container_cleanup"] = map[string]any{"complete": result == nil, "container": container.runner.name, "removed_owned_image": container.pulled && result == nil}
	return result
}

func (harness *harness) startFilesystemContainer() (container *filesystemContainer, result error) {
	return harness.startFilesystemContainerWithLimits(8*1024*1024, 32*1024*1024)
}

func (harness *harness) startFilesystemContainerWithLimits(faultBytes, healthyBytes int) (container *filesystemContainer, result error) {
	if harness.results["race_build"] == true {
		return nil, errors.New("filesystem fault service runs as Linux CGO_ENABLED=0; do not request a host race service build")
	}
	runner := &objectContainer{context: os.Getenv("STREAMD_DOCKER_CONTEXT"), name: "streamd-" + filepath.Base(harness.evidence)}
	if runner.context == "" {
		runner.context = "colima-native"
	}
	container = &filesystemContainer{runner: runner}
	defer func() {
		if result != nil {
			result = errors.Join(result, container.close(harness))
		}
	}()
	ctx, cancel := context.WithTimeout(harness.ctx, 2*time.Minute)
	defer cancel()
	body, err := runner.command(ctx, "info", "--format", "{{.OSType}}/{{.Architecture}}")
	if err != nil {
		return container, err
	}
	architecture := ""
	switch strings.TrimSpace(string(body)) {
	case "linux/aarch64", "linux/arm64":
		architecture = "arm64"
	case "linux/x86_64", "linux/amd64":
		architecture = "amd64"
	default:
		return container, fmt.Errorf("unsupported Docker platform %q", body)
	}
	binary := filepath.Join(harness.evidence, "streamd-linux")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/streamd")
	build.Dir = harness.root
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+architecture)
	if body, err := build.CombinedOutput(); err != nil {
		return container, fmt.Errorf("Linux service build: %w: %s", err, body)
	}
	if _, err := runner.command(ctx, "image", "inspect", filesystemImage); err != nil {
		if _, err := runner.command(ctx, "pull", filesystemImage); err != nil {
			return container, err
		}
		container.pulled = true
	}
	origin, err := url.Parse(harness.origin)
	if err != nil {
		return container, err
	}
	harness.settings.HTTP.Addr, harness.settings.RTMP.Addr = "0.0.0.0:8080", "0.0.0.0:1935"
	harness.settings.Application[0].Recorder.SaveTo, harness.settings.Application[1].Recorder.SaveTo = "/fault", "/healthy"
	config := filepath.Join(harness.evidence, "config.json")
	if err := writeJSON(config, harness.settings); err != nil {
		return container, err
	}
	identity := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	if os.Getuid() == 0 {
		return container, errors.New("filesystem container must run with an explicit non-root owner")
	}
	arguments := []string{"docker", "--context", runner.context, "run", "--name", runner.name, "--label", "io.streamd.test-run=" + runner.name,
		"--user", identity, "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--cpus", "1", "--memory", "512m", "--pids-limit", "128",
		"--tmpfs", fmt.Sprintf("/fault:rw,nosuid,nodev,size=%d,uid=%d,gid=%d,mode=0700", faultBytes, os.Getuid(), os.Getgid()),
		"--tmpfs", fmt.Sprintf("/healthy:rw,nosuid,nodev,size=%d,uid=%d,gid=%d,mode=0700", healthyBytes, os.Getuid(), os.Getgid()),
		"--mount", "type=bind,source=" + binary + ",target=/streamd,readonly", "--mount", "type=bind,source=" + config + ",target=/config.json,readonly",
		"--publish", "127.0.0.1:" + origin.Port() + ":8080", "--publish", fmt.Sprintf("127.0.0.1:%d:1935", harness.rtmpPort), filesystemImage, "/streamd", "-config", "/config.json"}
	harness.server, err = harness.launch("server", arguments...)
	if err != nil {
		return container, err
	}
	if err := harness.until("Linux service readiness", 20*time.Second, func() (bool, error) {
		status, _, err := harness.fetch("/readyz")
		return status == 200, err
	}); err != nil {
		return container, err
	}
	harness.started = time.Now()
	harness.results["filesystem_container"] = map[string]any{"name": runner.name, "image": filesystemImage, "platform": "linux/" + architecture, "user": identity, "service_cgo_enabled": false, "service_race_build": false, "fault_mount_bytes": faultBytes, "healthy_mount_bytes": healthyBytes, "data": "task-only tmpfs"}
	return container, nil
}

func (container *filesystemContainer) execute(harness *harness, arguments ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(harness.ctx, 10*time.Second)
	defer cancel()
	return container.runner.command(ctx, append([]string{"exec", container.runner.name}, arguments...)...)
}

func (container *filesystemContainer) collect(harness *harness) error {
	ctx, cancel := context.WithTimeout(harness.ctx, 15*time.Second)
	defer cancel()
	for _, mount := range []string{"fault", "healthy"} {
		destination := filepath.Join(harness.evidence, "recordings", mount)
		if err := os.MkdirAll(destination, 0700); err != nil {
			return err
		}
		command := exec.CommandContext(ctx, "docker", "--context", container.runner.context, "exec", container.runner.name, "tar", "-C", "/"+mount, "-cf", "-", ".")
		var stderr bytes.Buffer
		command.Stderr = &stderr
		body, err := command.Output()
		if err != nil || stderr.Len() != 0 {
			return fmt.Errorf("collect mounted recordings: %v: %s", err, stderr.String())
		}
		if len(body) > 40*1024*1024 {
			return errors.New("recording archive exceeds mount capacity")
		}
		if err := extractRecordings(bytes.NewReader(body), destination); err != nil {
			return err
		}
	}
	if _, err := container.runner.command(ctx, "kill", "--signal", "TERM", container.runner.name); err != nil {
		return err
	}
	return harness.finish("shutdown", harness.server, 15*time.Second)
}

func extractRecordings(source io.Reader, destination string) error {
	reader := tar.NewReader(source)
	count, total := 0, int64(0)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if !filepath.IsLocal(header.Name) {
			return fmt.Errorf("nonlocal recording archive path %q", header.Name)
		}
		path := filepath.Join(destination, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0700); err != nil {
				return err
			}
		case tar.TypeReg:
			total += header.Size
			if header.Size < 0 || total > 40*1024*1024 {
				return errors.New("recording archive exceeds mount capacity")
			}
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return err
			}
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(file, reader, header.Size)
			if err := errors.Join(copyErr, file.Close()); err != nil {
				return err
			}
			count++
		default:
			return fmt.Errorf("unsupported recording archive entry %q", header.Name)
		}
	}
	if count == 0 {
		return errors.New("recording archive contains no files")
	}
	return nil
}
