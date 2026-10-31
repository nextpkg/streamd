package config

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDecodeStrictValidation(t *testing.T) {
	for _, input := range []string{"typo: true", "http:\n  typo: true", "rtmp:\n  chunk_size: 1", "application:\n  - enable: true\n    app_name: same\n  - enable: true\n    app_name: same", "{}\n---\n{}", "log_level: not-a-level", "finder:\n  interval: 0"} {
		if _, err := Decode([]byte(input)); err == nil {
			t.Errorf("accepted invalid configuration: %s", input)
		}
	}
}

func TestConfigReloadAtomicRollback(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "service.yaml")
	write := func(content string) {
		t.Helper()
		if err := os.WriteFile(filename, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("log_level: INFO\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, err := NewManager(ctx, filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { Get().Revoke() }()
	GetFinder().Register("live", "existing")
	previous := Get()
	write("rtmp:\n  addr: 127.0.0.1:2935\n")
	if err := manager.Reload(); err == nil {
		t.Fatal("listener mutation accepted")
	}
	if Get() != previous || manager.Version() != 1 {
		t.Fatal("failed reload changed published snapshot")
	}
	write("finder:\n  redis:\n    address: 127.0.0.1:1\n")
	if err := manager.Reload(); err == nil {
		t.Fatal("unreachable candidate dependency accepted")
	}
	if Get() != previous || previous.Finder.runtime.ctx.Err() != nil {
		t.Fatal("failed preparation revoked active configuration")
	}
	write("log_level: DEBUG\n")
	if err := manager.Reload(); err != nil {
		t.Fatal(err)
	}
	if Get() == previous || manager.Version() != 2 || Get().LogLevel != "DEBUG" {
		t.Fatal("new snapshot was not committed")
	}
	if GetFinder().GetBackend("live", "existing") != "127.0.0.1" {
		t.Fatal("reload lost local discovery")
	}
	if previous.Finder.runtime.ctx.Err() == nil {
		t.Fatal("retired discovery context still active")
	}
}

func TestConfigWatcherAtomicRename(t *testing.T) {
	directory := t.TempDir()
	filename := filepath.Join(directory, "service.yaml")
	if err := os.WriteFile(filename, []byte("log_level: INFO\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, err := NewManager(ctx, filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { Get().Revoke() }()
	finished := make(chan error, 1)
	go func() { finished <- manager.Watch(ctx) }()
	select {
	case <-manager.watchReady:
	case err := <-finished:
		t.Fatalf("watcher setup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("watcher not ready")
	}
	temporary := filepath.Join(directory, "candidate.yaml")
	if err := os.WriteFile(temporary, []byte("log_level: WARN\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, filename); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for manager.Version() == 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if manager.Version() == 1 || Get().LogLevel != "WARN" {
		t.Fatal("atomic replacement did not reload")
	}
	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("watcher did not stop")
	}
}

func TestConfigurationSecretsAreRedacted(t *testing.T) {
	t.Setenv("STREAMD_TEST_SECRET", "do-not-expose")
	candidate, err := Decode([]byte("proxy:\n  secure_key: ${STREAMD_TEST_SECRET}\napplication:\n  - enable: true\n    app_name: live\n    static_pull:\n      - rtmp://user:password@localhost/live?token=hidden\n"))
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Proxy.SecureKey != "do-not-expose" {
		t.Fatal("credential environment expansion failed")
	}
	encoded, err := json.Marshal(candidate.Redacted())
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"do-not-expose", "password", "token=hidden"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("redacted configuration contains %q", secret)
		}
	}
}
