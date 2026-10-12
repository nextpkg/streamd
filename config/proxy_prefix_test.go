package config

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestProxyPrefixHistoryIsImmutableAndDeduplicated(test *testing.T) {
	initial := Proxy{Prefix: "/first/"}
	initial.preparePrefixes(nil)
	second := Proxy{Prefix: "second"}
	second.preparePrefixes(&initial)
	if initial.AcceptsPrefix("/second") || !second.AcceptsPrefix("/first") || !second.AcceptsPrefix("/second") {
		test.Fatal("published prefix snapshot was mutated or lost")
	}
	previous := &second
	for revision := 0; revision < 1000; revision++ {
		prefix := "first"
		if revision%2 == 0 {
			prefix = "/second/"
		}
		next := &Proxy{Prefix: prefix}
		next.preparePrefixes(previous)
		if len(next.prefixes) != 2 {
			test.Fatalf("repeated reload duplicated route metadata: %d", len(next.prefixes))
		}
		previous = next
	}
	encoded, err := json.Marshal(previous)
	if err != nil {
		test.Fatal(err)
	}
	var decoded Proxy
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		test.Fatal(err)
	}
	decoded.preparePrefixes(nil)
	if len(decoded.prefixes) != 1 || decoded.AcceptsPrefix("/second") {
		test.Fatal("process-local compatibility aliases leaked into persisted configuration")
	}
}

func TestProxyPrefixHistorySurvivesReloadButNotNewManager(test *testing.T) {
	filename := filepath.Join(test.TempDir(), "service.json")
	write := func(prefix string) {
		test.Helper()
		body, err := json.Marshal(map[string]any{"proxy": map[string]any{"prefix": prefix}})
		if err != nil {
			test.Fatal(err)
		}
		if err := os.WriteFile(filename, body, 0600); err != nil {
			test.Fatal(err)
		}
	}
	write("first")
	manager, err := NewManager(context.Background(), filename)
	if err != nil {
		test.Fatal(err)
	}
	defer func() { Get().Revoke() }()
	write("second")
	if err := manager.Reload(); err != nil {
		test.Fatal(err)
	}
	if !GetProxy().AcceptsPrefix("/first") || !GetProxy().AcceptsPrefix("/second") {
		test.Fatal("reload lost accepted route")
	}
	if _, err := NewManager(context.Background(), filename); err != nil {
		test.Fatal(err)
	}
	if GetProxy().AcceptsPrefix("/first") || !GetProxy().AcceptsPrefix("/second") {
		test.Fatal("fresh service inherited another manager's route history")
	}
}

func TestProxyPrefixRejectsUnroutablePaths(test *testing.T) {
	for _, prefix := range []string{"/", "//", ".", "..", "edge//nested", "edge/./nested", "edge/../nested"} {
		settings := Proxy{Prefix: prefix}
		if err := settings.validate(); err == nil {
			test.Errorf("accepted a prefix the HTTP router would rewrite: %q", prefix)
		}
	}
	for _, prefix := range []string{"edge", "/edge/", "hls", "edge/hls", "café/media"} {
		settings := Proxy{Prefix: prefix}
		if err := settings.validate(); err != nil {
			test.Errorf("valid prefix rejected: %q %v", prefix, err)
		}
	}
}
