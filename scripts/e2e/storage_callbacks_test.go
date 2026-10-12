package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"strings"
	"testing"
)

func TestStorageFaultCallbacksRequireFailedObjectAndExactSuccessfulFiles(test *testing.T) {
	for _, faultApp := range []string{"flv", "ts"} {
		for _, scenario := range []string{"valid", "false-success", "missing-failed-start", "duplicate-start", "missing-completion", "wrong-generation", "missing-publication", "duplicate-publication", "bad-format", "bad-connection", "extra-field", "reverse-time", "future-event", "http-failure", "duplicate-file", "unknown-file"} {
			test.Run(faultApp+"/"+scenario, func(test *testing.T) {
				healthy := "ts"
				if faultApp == "ts" {
					healthy = "flv"
				}
				failed := fmt.Sprintf("/%s/sample/1720000000.%s", faultApp, faultApp)
				files := []string{fmt.Sprintf("initial/%s/sample/1720000004.%s", healthy, healthy), fmt.Sprintf("recovered/%s/sample/1720000008.%s", faultApp, faultApp)}
				observations := []callbackObservation{}
				for _, app := range []string{"flv", "ts"} {
					for _, event := range []string{"OnPublish", "OnPublishDone"} {
						observations = append(observations, callbackObservation{Path: "/publication", Status: 200, Received: 1720000020000, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1720000000000)}})
					}
				}
				for _, identity := range append(append([]string{}, files...), "initial"+failed) {
					generation, relative, _ := strings.Cut(identity, "/")
					location := "/" + relative
					app := path.Ext(location)[1:]
					events := []string{"OnRecord", "OnRecordDone"}
					if location == failed {
						events = events[:1]
					}
					for index, event := range events {
						observations = append(observations, callbackObservation{Path: "/" + generation, Status: 200, Received: 1720000020000, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1720000000000 + index*1000), "location": location, "filename": path.Base(location), "connInfo": map[string]any{"app": app}}})
					}
				}
				switch scenario {
				case "false-success":
					falseSuccess := observations[len(observations)-1]
					falseSuccess.Payload = maps.Clone(falseSuccess.Payload)
					falseSuccess.Payload["event"] = "OnRecordDone"
					observations = append(observations, falseSuccess)
				case "missing-failed-start":
					observations = observations[:len(observations)-1]
				case "duplicate-start":
					observations = append(observations, observations[4])
				case "missing-completion":
					observations = append(observations[:5], observations[6:]...)
				case "wrong-generation":
					observations[5].Path = "/recovered"
				case "missing-publication":
					observations = observations[1:]
				case "duplicate-publication":
					observations = append(observations, observations[0])
				case "bad-format":
					observations[4].Payload["location"] = "/" + healthy + "/sample/wrong.mp4"
				case "bad-connection":
					observations[4].Payload["connInfo"] = map[string]any{"app": "other"}
				case "extra-field":
					observations[4].Payload["extra"] = true
				case "reverse-time":
					observations[4].Payload["occur"] = float64(1720000010000)
				case "future-event":
					observations[4].Received = 1719999999000
				case "http-failure":
					observations[4].Status = 503
				case "duplicate-file":
					files = append(files, files[0])
				case "unknown-file":
					files = append(files, "recovered/other/sample/1720000012.flv")
				}
				_, err := validateStorageFaultCallbacks(observations, files, failed, 1720000000000)
				if scenario == "valid" && err != nil {
					test.Fatal(err)
				}
				if scenario != "valid" && err == nil {
					test.Fatal("incomplete or contradictory storage callback evidence accepted")
				}
			})
		}
	}
}

func TestStorageFaultSelectionAndDiagnosticsKeepStreamIdentity(test *testing.T) {
	for _, app := range []string{"flv", "ts"} {
		test.Setenv("STREAMD_E2E_FAULT_APP", app)
		selected, healthy, err := storageFaultApplications()
		if err != nil || selected != app || healthy == app {
			test.Fatalf("wrong fault selection: %s %s %v", selected, healthy, err)
		}
		body, err := json.Marshal(map[string]string{"level": "ERROR", "msg": "[" + app + "/sample]recording failed: recording upload queue capacity exceeded"})
		if err != nil {
			test.Fatal(err)
		}
		match := func(message string) bool { return uploadErrorMatches("queue", message) }
		if _, err := faultErrorsForApp(body, app, match); err != nil {
			test.Fatal(err)
		}
		if _, err := faultErrorsForApp(body, healthy, match); err == nil {
			test.Fatal("fault attributed to the healthy recorder")
		}
	}
	test.Setenv("STREAMD_E2E_FAULT_APP", "other")
	if _, _, err := storageFaultApplications(); err == nil {
		test.Fatal("unknown fault application accepted")
	}
}
