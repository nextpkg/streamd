package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisImage = "docker.io/library/redis@sha256:b51665e66f00759be7c3152ad5ac3c66fb2f619c13ef62dea7cc1f9914524635"
const redisTestPassword = "streamd-e2e-only-password"

type redisContainer struct {
	runner  *objectContainer
	client  *redis.Client
	address string
	label   string
	pulled  bool
	created bool
}

func (harness *harness) startRedisContainer(label string) (container *redisContainer, result error) {
	runner := &objectContainer{context: os.Getenv("STREAMD_DOCKER_CONTEXT"), name: "streamd-" + filepath.Base(harness.evidence) + "-" + label}
	if runner.context == "" {
		runner.context = "colima-native"
	}
	container = &redisContainer{runner: runner, label: label}
	defer func() {
		if result != nil {
			result = errors.Join(result, container.close(harness))
		}
	}()
	ctx, cancel := context.WithTimeout(harness.ctx, 2*time.Minute)
	defer cancel()
	if _, err := runner.command(ctx, "image", "inspect", redisImage); err != nil {
		container.pulled = true
		body, err := runner.command(ctx, "pull", redisImage)
		if writeErr := os.WriteFile(filepath.Join(harness.evidence, "redis-"+label+"-pull.log"), body, 0600); writeErr != nil {
			return container, writeErr
		}
		if err != nil {
			return container, err
		}
	}
	identity := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	if os.Getuid() == 0 {
		return container, errors.New("redis E2E requires a non-root container owner")
	}
	port, err := freePort()
	if err != nil {
		return container, err
	}
	_, err = runner.command(ctx, "run", "--detach", "--name", runner.name, "--label", "io.streamd.test-run="+runner.name,
		"--user", identity, "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--cpus", "0.5", "--memory", "128m", "--pids-limit", "64",
		"--tmpfs", fmt.Sprintf("/data:rw,nosuid,nodev,size=8m,uid=%d,gid=%d,mode=0700", os.Getuid(), os.Getgid()),
		"--publish", fmt.Sprintf("127.0.0.1:%d:6379", port), redisImage, "redis-server", "--save", "", "--appendonly", "no", "--bind", "0.0.0.0", "--protected-mode", "yes",
		"--requirepass", redisTestPassword, "--notify-keyspace-events", "KEA", "--loglevel", "warning")
	if err != nil {
		return container, err
	}
	container.created = true
	body, err := runner.command(ctx, "port", runner.name, "6379/tcp")
	if err != nil {
		return container, err
	}
	container.address = strings.TrimSpace(string(body))
	if !strings.HasPrefix(container.address, "127.0.0.1:") || strings.Contains(container.address, "\n") {
		return container, fmt.Errorf("unexpected Redis binding %q", container.address)
	}
	container.client = redis.NewClient(&redis.Options{Addr: container.address, Password: redisTestPassword, DB: 13, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, MaxRetries: -1, ContextTimeoutEnabled: true})
	if err := harness.until("Redis "+label+" ready", 15*time.Second, func() (bool, error) { return container.client.Ping(harness.ctx).Err() == nil, nil }); err != nil {
		return container, err
	}
	info, err := container.client.Info(harness.ctx, "server").Result()
	if err != nil {
		return container, err
	}
	harness.results["redis_container_"+label] = map[string]any{"image": redisImage, "name": runner.name, "address": container.address, "user": identity, "db": 13, "persistence": false, "data": "task-only tmpfs", "server_info": info}
	return container, nil
}

func (container *redisContainer) close(harness *harness) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var result error
	if container.client != nil {
		result = errors.Join(result, container.client.Close())
	}
	if container.created {
		body, err := container.runner.command(ctx, "logs", container.runner.name)
		result = errors.Join(result, err, os.WriteFile(filepath.Join(harness.evidence, "redis-"+container.label+"-server.log"), body, 0600))
		_, err = container.runner.command(ctx, "rm", "--force", "--volumes", container.runner.name)
		result = errors.Join(result, err)
	}
	if container.pulled {
		_, err := container.runner.command(ctx, "image", "rm", redisImage)
		result = errors.Join(result, err)
	}
	harness.results["redis_cleanup_"+container.label] = map[string]any{"complete": result == nil, "container": container.runner.name, "removed_owned_image": container.pulled && result == nil}
	return result
}
