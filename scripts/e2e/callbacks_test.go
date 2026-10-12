package main

import (
	"fmt"
	"path/filepath"
	"testing"
)

func callbackEvidenceFixture(directory string) ([]callbackObservation, []string) {
	observations := []callbackObservation{}
	files := []string{}
	for _, app := range []string{"flv", "ts"} {
		for _, event := range []string{"OnPublish", "OnPublishDone"} {
			status := 200
			if event == "OnPublishDone" {
				status = 0
			}
			observations = append(observations, callbackObservation{Path: "/initial", Received: 1001, Status: status, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1000)}})
		}
		for index := range 10 {
			filename := fmt.Sprintf("%d.%s", index, app)
			files = append(files, filepath.Join(directory, app, "sample", filename))
			target := "/initial"
			if index >= 5 {
				target = "/rotated"
			}
			for _, event := range []string{"OnRecord", "OnRecordDone"} {
				observations = append(observations, callbackObservation{Path: target, Received: 1001, Status: 200, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1000), "location": "/" + app + "/sample/" + filename, "filename": filename, "connInfo": map[string]any{"app": app}}})
			}
		}
	}
	return observations, files
}

func TestCallbackEvidenceRequiresExactPayloadCountsFilesAndSnapshots(t *testing.T) {
	directory := t.TempDir()
	observations, files := callbackEvidenceFixture(directory)
	if _, err := validateCallbackObservations(observations, files, 1000, directory); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"duplicate", "missing", "unknown-event", "timestamp", "filename", "connection", "target", "status", "unknown-field", "file-mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			observations, files := callbackEvidenceFixture(directory)
			switch scenario {
			case "duplicate":
				observations = append(observations, observations[2])
			case "missing":
				observations = observations[1:]
			case "unknown-event":
				observations[2].Payload["event"] = "not-known"
			case "timestamp":
				observations[2].Payload["occur"] = float64(2000)
			case "filename":
				observations[2].Payload["filename"] = "wrong.flv"
			case "connection":
				observations[2].Payload["connInfo"] = nil
			case "target":
				observations[3].Path = "/rotated"
			case "status":
				observations[2].Status = 404
			case "unknown-field":
				observations[0].Payload["extra"] = true
			case "file-mismatch":
				files[0] = filepath.Join(directory, "unknown.flv")
			}
			if _, err := validateCallbackObservations(observations, files, 1000, directory); err == nil {
				t.Fatal("incomplete callback evidence was accepted")
			}
		})
	}
}

func TestCallbackErrorEvidenceRequiresExactInjectedFailures(t *testing.T) {
	rejected := `{"level":"ERROR","msg":"[flv/sample]err=callback returned HTTP 503,event=OnRecord"}` + "\n"
	cancelled := `{"level":"ERROR","msg":"[ts/sample]err=callback request failed,event=OnPublishDone"}` + "\n"
	if err := validateCallbackServerErrors([]byte(rejected+cancelled), 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := validateCallbackServerErrors([]byte(rejected+cancelled+`{"level":"WARN","msg":"Not continuous timestamp"}`), 1, 1); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{rejected, rejected + cancelled + cancelled, rejected + cancelled + `{"level":"ERROR","msg":"recording failed"}`, rejected + cancelled + `{"level":"ERROR","msg":"Not continuous timestamp"}`, rejected + cancelled + `{"level":"WARN","msg":"unknown warning"}`, `{"level":"ERROR","msg":"[unknown/sample]err=callback request failed,event=OnPublishDone"}`} {
		if err := validateCallbackServerErrors([]byte(body), 1, 1); err == nil {
			t.Fatalf("accepted missing, duplicate or unrelated callback failure: %s", body)
		}
	}
}
