package config

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCallbackCandidateRollbackAndRemoval(test *testing.T) {
	directory := test.TempDir()
	filename := filepath.Join(directory, "settings.json")
	settings := map[string]any{"application": []any{map[string]any{"enable": true, "app_name": "events", "event": map[string]any{"on_publish": "http://127.0.0.1:1/old-start", "on_publish_done": "http://127.0.0.1:1/old-end"}, "recorder": map[string]any{"enable": true, "save_to": directory, "save_as": "flv", "save_mode": "immediate", "segment": 4, "driver": "fs", "event": map[string]any{"on_record": "http://127.0.0.1:1/old-open", "on_record_done": "http://127.0.0.1:1/old-close"}}}}}
	write := func() {
		test.Helper()
		body, err := json.Marshal(settings)
		if err != nil {
			test.Fatal(err)
		}
		if err := os.WriteFile(filename, body, 0600); err != nil {
			test.Fatal(err)
		}
	}
	write()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, err := NewManager(ctx, filename)
	if err != nil {
		test.Fatal(err)
	}
	defer func() { Get().Revoke() }()
	previous := Get()
	previousApp := previous.GetApp("events")
	app := settings["application"].([]any)[0].(map[string]any)
	publication := app["event"].(map[string]any)
	recording := app["recorder"].(map[string]any)["event"].(map[string]any)
	for _, field := range []string{"on_publish", "on_publish_done", "on_record", "on_record_done"} {
		events := publication
		if field == "on_record" || field == "on_record_done" {
			events = recording
		}
		original := events[field]
		events[field] = "file:///invalid-callback"
		write()
		if err := manager.Reload(); err == nil || manager.Version() != 1 || Get() != previous || previous.GetApp("events") != previousApp || previous.Finder.runtime.ctx.Err() != nil {
			test.Fatalf("invalid %s changed the active snapshot: %v", field, err)
		}
		events[field] = original
	}
	app["event"] = map[string]any{}
	app["recorder"].(map[string]any)["event"] = map[string]any{}
	write()
	if err := manager.Reload(); err != nil {
		test.Fatal(err)
	}
	current := Get().GetApp("events")
	if manager.Version() != 2 || current.Event != (AppEvent{}) || current.Recorder.Event != (RecorderEvent{}) || previousApp.Event.OnPublish != "http://127.0.0.1:1/old-start" || previousApp.Recorder.Event.OnRecordDone != "http://127.0.0.1:1/old-close" {
		test.Fatal("callback removal mutated retained event snapshots or failed to commit")
	}
}
