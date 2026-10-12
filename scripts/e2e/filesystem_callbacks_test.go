package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
)

func filesystemCallbackFixture(opened bool) ([]callbackObservation, []string, map[string]bool) {
	observations, files := []callbackObservation{}, []string{}
	for _, app := range []string{"flv", "ts"} {
		for _, event := range []string{"OnPublish", "OnPublishDone"} {
			observations = append(observations, callbackObservation{Path: "/publication", Received: 3000, Status: 200, Payload: map[string]any{"key": app + "/sample", "event": event, "occur": float64(2000)}})
		}
		filename := "/" + app + "/sample/2." + app
		files = append(files, filename)
		for _, event := range []string{"OnRecord", "OnRecordDone"} {
			observations = append(observations, callbackObservation{Path: "/filesystem", Received: 3000, Status: 200, Payload: map[string]any{"key": app + "/sample", "event": event, "occur": float64(2000), "location": filename, "filename": path.Base(filename), "connInfo": map[string]any{"app": app}}})
		}
	}
	failed := "/flv/sample/1.flv"
	if opened {
		files = append(files, failed)
		observations = append(observations, callbackObservation{Path: "/filesystem", Received: 3000, Status: 200, Payload: map[string]any{"key": "flv/sample", "event": "OnRecord", "occur": float64(1500), "location": failed, "filename": path.Base(failed), "connInfo": map[string]any{"app": "flv"}}})
	}
	return observations, files, map[string]bool{failed: opened}
}

func TestFilesystemCallbacksRequireIndependentFailureAndFileIdentity(test *testing.T) {
	for _, opened := range []bool{false, true} {
		for _, scenario := range []string{"valid", "false-success", "missing-publisher", "missing-recording", "duplicate-event", "duplicate-file", "unknown-file", "wrong-target", "wrong-time", "wrong-format", "wrong-connection", "http-failure", "failure-not-observed"} {
			test.Run(fmt.Sprintf("opened=%t/%s", opened, scenario), func(test *testing.T) {
				observations, files, failed := filesystemCallbackFixture(opened)
				switch scenario {
				case "false-success":
					observations = append(observations, callbackObservation{Path: "/filesystem", Received: 3000, Status: 200, Payload: map[string]any{"key": "flv/sample", "event": "OnRecordDone", "occur": float64(2500), "location": "/flv/sample/1.flv", "filename": "1.flv", "connInfo": map[string]any{"app": "flv"}}})
				case "missing-publisher":
					observations = observations[1:]
				case "missing-recording":
					observations = append(observations[:3], observations[4:]...)
				case "duplicate-event":
					observations = append(observations, observations[2])
				case "duplicate-file":
					files = append(files, files[0])
				case "unknown-file":
					files[0] = "/flv/sample/3.flv"
				case "wrong-target":
					observations[2].Path = "/other"
				case "wrong-time":
					observations[3].Payload["occur"] = float64(1000)
				case "wrong-format":
					observations[2].Payload["location"] = "/flv/sample/2.ts"
				case "wrong-connection":
					observations[2].Payload["connInfo"] = map[string]any{"app": "other"}
				case "http-failure":
					observations[2].Status = 503
				case "failure-not-observed":
					failed = nil
				}
				_, err := validateFilesystemCallbacks(observations, files, failed, 1000, 0, 0)
				if (err == nil) != (scenario == "valid") {
					test.Fatalf("unexpected validation: %v", err)
				}
			})
		}
	}
}

func TestFilesystemCallbacksUseMeasuredClockWindow(test *testing.T) {
	observations, files, failed := filesystemCallbackFixture(false)
	observations[0].Received = 1980
	if _, err := validateFilesystemCallbacks(observations, files, failed, 1000, 0, 0); err == nil {
		test.Fatal("future callback accepted without clock evidence")
	}
	lower, upper, err := filesystemClockBounds(filesystemClockSample{BeforeNS: 1000000000, GuestNS: 1030000000, AfterNS: 1004000000})
	if err != nil {
		test.Fatal(err)
	}
	if _, err := validateFilesystemCallbacks(observations, files, failed, 1000, lower, upper); err != nil {
		test.Fatal(err)
	}
	observations[0].Payload["occur"] = float64(2300)
	if _, err := validateFilesystemCallbacks(observations, files, failed, 1000, lower, upper); err == nil {
		test.Fatal("callback outside measured clock window accepted")
	}
	for _, sample := range []filesystemClockSample{{BeforeNS: 2000000000, GuestNS: 2000000000, AfterNS: 1000000000}, {BeforeNS: 1000000000, GuestNS: 4000000000, AfterNS: 1004000000}, {BeforeNS: 1000000000, GuestNS: 1001000000, AfterNS: 3000000000}} {
		if _, _, err := filesystemClockBounds(sample); err == nil {
			test.Fatal("clock jump or unbounded sample accepted")
		}
	}
}

func TestFilesystemRollbackSupportsTransportStreamPaths(test *testing.T) {
	directory := test.TempDir()
	filename := filepath.Join(directory, "recordings/fault/ts/sample/123.ts")
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(filename, make([]byte, 188), 0600); err != nil {
		test.Fatal(err)
	}
	files, err := verifyFilesystemRollback(directory, []string{"recording append failed: written=4096 requested=100000 rollback_offset=188: write /fault/ts/sample/123.ts: no space left on device"})
	if err != nil || len(files) != 1 || files[0]["accepted_bytes"] != int64(188) {
		test.Fatalf("TS rollback missing: %v %v", files, err)
	}
}
