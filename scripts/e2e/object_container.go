package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const objectImage = "docker.io/chrislusf/seaweedfs@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d"

type objectContainer struct {
	context  string
	name     string
	endpoint string
	pulled   bool
	created  bool
}

func (container *objectContainer) command(ctx context.Context, arguments ...string) ([]byte, error) {
	args := append([]string{"--context", container.context}, arguments...)
	body, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return body, fmt.Errorf("docker %v: %w: %s", arguments, err, body)
	}
	return body, nil
}

func (harness *harness) startObjectContainer() (container *objectContainer, result error) {
	container = &objectContainer{context: os.Getenv("STREAMD_DOCKER_CONTEXT"), name: "streamd-" + filepath.Base(harness.evidence)}
	if container.context == "" {
		container.context = "colima-native"
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, container.close(harness))
		}
	}()
	ctx, cancel := context.WithTimeout(harness.ctx, 2*time.Minute)
	defer cancel()
	if _, err := container.command(ctx, "image", "inspect", objectImage); err != nil {
		container.pulled = true
		body, err := container.command(ctx, "pull", objectImage)
		if writeErr := os.WriteFile(filepath.Join(harness.evidence, "object-pull.log"), body, 0600); writeErr != nil {
			return container, writeErr
		}
		if err != nil {
			return container, err
		}
	}
	config := filepath.Join(harness.evidence, "object-credentials.json")
	if err := writeJSON(config, map[string]any{"identities": []any{map[string]any{"name": "test-admin", "credentials": []any{map[string]string{"accessKey": "test-id", "secretKey": "test-secret"}, map[string]string{"accessKey": "test-next-id", "secretKey": "test-next-secret"}}, "actions": []string{"Admin", "Read", "Write", "List", "Tagging"}}}}); err != nil {
		return container, err
	}
	_, err := container.command(ctx, "run", "--detach", "--name", container.name, "--label", "io.streamd.test-run="+container.name,
		"--entrypoint", "/usr/bin/weed", "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--cpus", "1", "--memory", "1g", "--pids-limit", "256",
		"--env", "GOMEMLIMIT=768MiB", "--tmpfs", fmt.Sprintf("/data:rw,nosuid,nodev,size=512m,uid=%d,gid=%d,mode=0700", os.Getuid(), os.Getgid()), "--tmpfs", "/tmp:rw,nosuid,nodev,size=64m,mode=1777",
		"--mount", "type=bind,source="+config+",target=/config/s3.json,readonly", "--publish", "127.0.0.1::8333", objectImage,
		"mini", "-dir=/data", "-s3.config=/config/s3.json", "-bucket=test-bucket", "-admin.ui=false", "-webdav=false", "-master.telemetry=false", "-volume.max=3")
	if err != nil {
		return container, err
	}
	container.created = true
	body, err := container.command(ctx, "port", container.name, "8333/tcp")
	if err != nil {
		return container, err
	}
	address := strings.TrimSpace(string(body))
	if !strings.HasPrefix(address, "127.0.0.1:") || strings.Contains(address, "\n") {
		return container, fmt.Errorf("unexpected S3 binding %q", address)
	}
	container.endpoint = "http://" + address
	harness.results["object_container"] = map[string]any{"image": objectImage, "name": container.name, "context": container.context, "endpoint": container.endpoint, "data": "task-only tmpfs", "telemetry": false}
	return container, nil
}

func (container *objectContainer) close(harness *harness) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var result error
	if container.created {
		body, err := container.command(ctx, "logs", container.name)
		result = errors.Join(result, err, os.WriteFile(filepath.Join(harness.evidence, "object-server.log"), body, 0600))
		_, err = container.command(ctx, "rm", "--force", "--volumes", container.name)
		result = errors.Join(result, err)
	}
	removedImage := false
	if container.pulled {
		if _, err := container.command(ctx, "image", "inspect", objectImage); err == nil {
			_, err = container.command(ctx, "image", "rm", objectImage)
			result = errors.Join(result, err)
			removedImage = err == nil
		}
	}
	harness.results["object_container_cleanup"] = map[string]any{"complete": result == nil, "removed_owned_image": removedImage, "image_pulled_by_runner": container.pulled, "container": container.name}
	return result
}
