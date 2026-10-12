package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func takeoverCallbackFixture(directory string) ([]callbackObservation, []string) {
	observations := []callbackObservation{}
	files := []string{}
	for _, app := range []string{"flv", "ts"} {
		for generation := range 3 {
			for _, event := range []string{"OnPublish", "OnPublishDone"} {
				observations = append(observations, callbackObservation{Path: fmt.Sprintf("/generation/%d", generation), Received: 1001, Status: 200, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1000)}})
			}
		}
		for index := range 2 {
			filename := fmt.Sprintf("%d.%s", index, app)
			files = append(files, filepath.Join(directory, app, "sample", filename))
			for _, event := range []string{"OnRecord", "OnRecordDone"} {
				observations = append(observations, callbackObservation{Path: "/recording", Received: 1001, Status: 200, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1000), "location": "/" + app + "/sample/" + filename, "filename": filename, "connInfo": map[string]any{"app": app}}})
			}
		}
	}
	for range 2 {
		for _, event := range []string{"OnPublish", "OnPublishDone"} {
			observations = append(observations, callbackObservation{Path: "/ordinary", Received: 1001, Status: 200, Payload: map[string]any{"event": event, "key": "ordinary/sample", "occur": float64(1000)}})
		}
	}
	return observations, files
}

func TestTakeoverCallbackEvidenceRequiresGenerationsAndRecordingPairs(test *testing.T) {
	directory := test.TempDir()
	observations, files := takeoverCallbackFixture(directory)
	if _, err := validateTakeoverCallbacks(observations, files, directory); err != nil {
		test.Fatal(err)
	}
	for _, scenario := range []string{"missing", "duplicate", "generation", "file", "ordinary", "unknown-field", "connection"} {
		test.Run(scenario, func(test *testing.T) {
			observations, files := takeoverCallbackFixture(directory)
			switch scenario {
			case "missing":
				observations = observations[1:]
			case "duplicate":
				observations = append(observations, observations[0])
			case "generation":
				observations[1].Path = "/generation/1"
			case "file":
				files[0] = filepath.Join(directory, "not-recorded.flv")
			case "ordinary":
				observations = observations[:len(observations)-1]
			case "unknown-field":
				observations[0].Payload["extra"] = true
			case "connection":
				observations[6].Payload["connInfo"] = nil
			}
			if _, err := validateTakeoverCallbacks(observations, files, directory); err == nil {
				test.Fatal("invalid takeover evidence accepted")
			}
		})
	}
}

func pressureCallbackFixture() ([]callbackObservation, []string) {
	observations := []callbackObservation{}
	cancelled := []string{}
	appendEvent := func(key, target string, status int) {
		observations = append(observations, callbackObservation{Path: target, Received: 1001, Status: status, Payload: map[string]any{"event": "OnPublish", "key": key, "occur": float64(1000)}})
	}
	appendEvent("flv/sample", "/normal", 200)
	appendEvent("ts/sample", "/normal", 200)
	for index := range 32 {
		appendEvent(fmt.Sprintf("pressure/burst-%02d", index), "/hold", 200)
	}
	appendEvent("pressure/recovered", "/hold", 200)
	for index := range 2 {
		key := fmt.Sprintf("pressure/shutdown-%d", index)
		appendEvent(key, "/hold", 0)
		cancelled = append(cancelled, key+"/OnPublish")
	}
	return observations, cancelled
}

func TestPressureCallbackEvidenceRequiresOverflowIsolationRecoveryAndCancellation(test *testing.T) {
	observations, cancelled := pressureCallbackFixture()
	if err := validateCallbackStressObservations(observations, cancelled); err != nil {
		test.Fatal(err)
	}
	for _, scenario := range []string{"missing", "duplicate", "overflow-delivered", "missing-recovery", "not-acknowledged", "missing-cancellation", "duplicate-cancellation", "unrelated-cancellation", "unknown-field"} {
		test.Run(scenario, func(test *testing.T) {
			observations, cancelled := pressureCallbackFixture()
			switch scenario {
			case "missing":
				observations = observations[1:]
			case "duplicate":
				observations = append(observations, observations[0])
			case "overflow-delivered":
				observations[2].Payload["key"] = "pressure/overflow"
			case "missing-recovery":
				observations = append(observations[:34], observations[35:]...)
			case "not-acknowledged":
				observations[2].Status = 0
			case "missing-cancellation":
				cancelled = cancelled[1:]
			case "duplicate-cancellation":
				cancelled = append(cancelled, cancelled[0])
			case "unrelated-cancellation":
				cancelled[0] = "pressure/burst-00/OnPublish"
			case "unknown-field":
				observations[2].Payload["extra"] = true
			}
			if err := validateCallbackStressObservations(observations, cancelled); err == nil {
				test.Fatal("invalid pressure evidence accepted")
			}
		})
	}
}

func TestPressureCallbackErrorsRequireExactlyOwnedRejectionAndCancellations(test *testing.T) {
	messages := []string{"[pressure/overflow]err=callback concurrency capacity exceeded,event=OnPublish", "[pressure/shutdown-0]err=callback request failed,event=OnPublish", "[pressure/shutdown-1]err=callback request failed,event=OnPublish"}
	for _, app := range []string{"flv", "ts"} {
		for _, event := range []string{"OnPublishDone", "OnRecordDone"} {
			messages = append(messages, fmt.Sprintf("[%s/sample]err=callback request failed,event=%s", app, event))
		}
	}
	var lines []string
	for _, message := range messages {
		body, err := json.Marshal(map[string]string{"level": "ERROR", "msg": message})
		if err != nil {
			test.Fatal(err)
		}
		lines = append(lines, string(body))
	}
	if _, err := validateCallbackStressErrors([]byte(strings.Join(lines, "\n"))); err != nil {
		test.Fatal(err)
	}
	for _, bad := range []string{strings.Join(lines[1:], "\n"), strings.Join(append(append([]string(nil), lines...), lines[0]), "\n"), strings.Join(lines, "\n") + "\n" + `{"level":"ERROR","msg":"recording failed"}`} {
		if _, err := validateCallbackStressErrors([]byte(bad)); err == nil {
			test.Fatal("incomplete or extra failure was accepted")
		}
	}
}
