package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"testing"

	"github.com/nextpkg/streamd/internal/teststore"
)

func completionCallbackFixture() ([]callbackObservation, []string, []string) {
	observations, files, cloud := []callbackObservation{}, []string{}, []string{}
	for _, app := range []string{"flv", "ts"} {
		cloud = append(cloud, app+"/sample/1720000000."+app)
		files = append(files, app+"/sample/1720000020."+app)
		for session := 0; session < 2; session++ {
			for _, event := range []string{"OnPublish", "OnPublishDone"} {
				observations = append(observations, callbackObservation{Path: "/publication", Received: 1720000080000, Status: 200, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1720000000000 + session*10000)}})
			}
		}
		for index, event := range []string{"OnRecord", "OnRecord", "OnRecord", "OnRecordDone"} {
			location, endpoint := "/"+cloud[len(cloud)-1], "/initial"
			if index >= 2 {
				location, endpoint = "/"+files[len(files)-1], "/recovered"
			}
			observations = append(observations, callbackObservation{Path: endpoint, Received: 1720000080000, Status: 200, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1720000000000 + index*10000), "filename": path.Base(location), "location": location, "connInfo": map[string]any{"app": app}}})
		}
	}
	return observations, files, cloud
}

func TestCompletionCallbacksDistinguishPausedFailedAndRecoveredFiles(test *testing.T) {
	for _, scenario := range []string{"valid", "false-success", "missing-resume", "duplicate-success", "missing-publisher", "wrong-target", "wrong-connection", "unknown-file", "duplicate-file", "wrong-time", "wrong-status"} {
		test.Run(scenario, func(test *testing.T) {
			observations, files, cloud := completionCallbackFixture()
			switch scenario {
			case "false-success":
				observations[5].Payload["event"] = "OnRecordDone"
			case "missing-resume":
				observations = append(observations[:5], observations[6:]...)
			case "duplicate-success":
				observations = append(observations, observations[7])
			case "missing-publisher":
				observations = observations[1:]
			case "wrong-target":
				observations[7].Path = "/initial"
			case "wrong-connection":
				observations[7].Payload["connInfo"] = map[string]any{"app": "other"}
			case "unknown-file":
				files[0] = "flv/sample/1720000040.flv"
			case "duplicate-file":
				files = append(files, files[0])
			case "wrong-time":
				observations[7].Payload["occur"] = float64(1720000010000)
			case "wrong-status":
				observations[7].Status = 503
			}
			_, err := validateCompletionCallbacks(observations, files, cloud, 1720000000000)
			if scenario == "valid" && err != nil {
				test.Fatal(err)
			}
			if scenario != "valid" && err == nil {
				test.Fatal("contradictory callback evidence accepted")
			}
		})
	}
}

func TestCompletionObjectsRequireCommitAndImmutablePayload(test *testing.T) {
	for _, scenario := range []string{"valid", "checkpoint-changed", "same-size-other-generation", "not-committed", "recreated", "aborted", "no-drop", "unexpected-delete", "missing-object", "unknown-mode"} {
		test.Run(scenario, func(test *testing.T) {
			saved := map[string][]byte{"flv/sample/.streamd-checkpoints/first.json": []byte("flv-state"), "ts/sample/.streamd-checkpoints/second.json": []byte("ts-state")}
			completed := maps.Clone(saved)
			outcomes := map[string]teststore.CompletionOutcome{}
			for _, app := range []string{"flv", "ts"} {
				key := app + "/sample/1720000000." + app
				completed[key] = []byte("media")
				outcomes[key] = teststore.CompletionOutcome{Created: 1, SuccessfulCompletions: 1, CompletionManifestSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("manifest"))), Dropped: 1, UploadID: "id", Parts: map[int]teststore.PartDigest{1: {Bytes: 5, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("media")))}}}
			}
			key, mode := "flv/sample/1720000000.flv", "response-lost"
			outcome := outcomes[key]
			switch scenario {
			case "checkpoint-changed":
				completed["flv/sample/.streamd-checkpoints/first.json"] = []byte("changed")
			case "same-size-other-generation":
				completed[key] = []byte("other")
			case "not-committed":
				outcome.SuccessfulCompletions = 0
			case "recreated":
				outcome.Created = 2
			case "aborted":
				outcome.Aborted = 1
			case "no-drop":
				outcome.Dropped = 0
			case "unexpected-delete":
				outcome.Denied = 1
			case "missing-object":
				delete(completed, key)
			case "unknown-mode":
				mode = "unknown"
			}
			outcomes[key] = outcome
			err := validateCompletionObjects(saved, completed, outcomes, mode)
			if scenario == "valid" && err != nil {
				test.Fatal(err)
			}
			if scenario != "valid" && err == nil {
				test.Fatal("unproven remote completion accepted")
			}
		})
	}
}

func TestCompletionObjectsPermitVerifiedIdempotentRetries(test *testing.T) {
	saved := map[string][]byte{"flv/sample/.streamd-checkpoints/first.json": []byte("flv-state"), "ts/sample/.streamd-checkpoints/second.json": []byte("ts-state")}
	completed := maps.Clone(saved)
	outcomes := map[string]teststore.CompletionOutcome{}
	for _, app := range []string{"flv", "ts"} {
		key := app + "/sample/1720000000." + app
		completed[key] = []byte("media")
		outcomes[key] = teststore.CompletionOutcome{Created: 1, SuccessfulCompletions: 2, CompletionManifestSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("manifest"))), Dropped: 2, UploadID: "same-upload-id", Parts: map[int]teststore.PartDigest{1: {Bytes: 5, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("media")))}}}
	}
	if err := validateCompletionObjects(saved, completed, outcomes, "response-lost"); err != nil {
		test.Fatal(err)
	}
}

func TestCompletionErrorsRequireBothKnownRecorderFailures(test *testing.T) {
	for _, mode := range []string{"response-lost", "cleanup-denied"} {
		cause := "NoSuchUpload"
		if mode == "cleanup-denied" {
			cause = "remove completed recording checkpoint: AccessDenied"
		}
		var body []byte
		for _, prefix := range []string{"close failed,sync=true,block=true: ", "close failed,sync=true,block=true: ", "[flv/sample]recording configuration finalization: close failed,sync=true,block=true: ", "[ts/sample]recording configuration finalization: close failed,sync=true,block=true: "} {
			line, err := json.Marshal(map[string]string{"level": "ERROR", "msg": prefix + cause})
			if err != nil {
				test.Fatal(err)
			}
			body = append(body, append(line, '\n')...)
		}
		if _, err := completionOutcomeErrors(body, mode); err != nil {
			test.Fatal(err)
		}
		for _, invalid := range [][]byte{nil, append(append([]byte{}, body...), body...), append(append([]byte{}, body...), []byte("{\"level\":\"ERROR\",\"msg\":\"unrelated failure\"}\n")...)} {
			if _, err := completionOutcomeErrors(invalid, mode); err == nil {
				test.Fatal("missing or extra failure accounting accepted")
			}
		}
	}
}
