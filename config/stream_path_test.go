package config

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestApplicationPathReloadRejectsWithoutCommit(test *testing.T) {
	filename := filepath.Join(test.TempDir(), "service.json")
	write := func(name string) {
		test.Helper()
		body, err := json.Marshal(map[string]any{"application": []map[string]any{{"enable": true, "app_name": name}}})
		if err != nil {
			test.Fatal(err)
		}
		if err := os.WriteFile(filename, body, 0600); err != nil {
			test.Fatal(err)
		}
	}
	write("live")
	manager, err := NewManager(context.Background(), filename)
	if err != nil {
		test.Fatal(err)
	}
	defer Get().Revoke()
	previous := Get()
	for _, name := range []string{"", ".", "..", "live/other", "live\\other", "live\x00", "live\n", "live\x7f"} {
		write(name)
		if err := manager.Reload(); err == nil || Get() != previous || manager.Version() != 1 {
			test.Fatalf("invalid application %q changed configuration: version=%d error=%v", name, manager.Version(), err)
		}
	}
	for _, name := range []string{"直播", "camera one", "live.v1", "%2e%2e"} {
		write(name)
		if err := manager.Reload(); err != nil {
			test.Fatalf("literal application %q rejected: %v", name, err)
		}
	}
	if err := (&App{Enable: false}).validate(); err != nil {
		test.Fatal("disabled unnamed application changed behavior:", err)
	}
}
