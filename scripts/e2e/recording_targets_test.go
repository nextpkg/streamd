package main

import (
	"fmt"
	"path"
	"strings"
	"testing"
)

func targetCallbackFixture() ([]callbackObservation, []string) {
	var observations []callbackObservation
	var files []string
	for _, generation := range recordingTargetGenerations {
		for _, app := range []string{"flv", "ts"} {
			format := app
			if generation.Swapped {
				if format == "flv" {
					format = "ts"
				} else {
					format = "flv"
				}
			}
			location := fmt.Sprintf("/%s/sample/1720000000.%s", app, format)
			files = append(files, generation.Name+location)
			for index, event := range []string{"OnRecord", "OnRecordDone"} {
				observations = append(observations, callbackObservation{Path: "/" + generation.Name, Received: 1720000010000, Status: 200, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1720000000000 + index*1000), "filename": path.Base(location), "location": location, "connInfo": map[string]any{"app": app}}})
			}
		}
	}
	return observations, files
}

func TestRecordingTargetCallbacksRequireExactGenerationFilePairs(test *testing.T) {
	for _, scenario := range []string{"valid", "duplicate", "missing-done", "wrong-target", "wrong-format", "unaligned-round", "wrong-key", "wrong-connection", "reverse-time", "future-event", "extra-file", "duplicate-file", "missing-generation", "http-failure"} {
		test.Run(scenario, func(test *testing.T) {
			observations, files := targetCallbackFixture()
			switch scenario {
			case "duplicate":
				observations = append(observations, observations[0])
			case "missing-done":
				observations = observations[1:]
			case "wrong-target":
				observations[1].Path = "/aws-round"
			case "wrong-format":
				observations[0].Payload["location"], observations[0].Payload["filename"] = "/flv/sample/1720000000.ts", "1720000000.ts"
			case "unaligned-round":
				observations[0].Payload["location"], observations[0].Payload["filename"] = "/flv/sample/1720000001.flv", "1720000001.flv"
			case "wrong-key":
				observations[0].Payload["key"] = "other/sample"
			case "wrong-connection":
				observations[0].Payload["connInfo"] = map[string]any{"app": "other"}
			case "reverse-time":
				observations[0].Payload["occur"] = float64(1720000002000)
			case "future-event":
				observations[0].Received = 1719999999000
			case "extra-file":
				files = append(files, "fs-initial/flv/sample/1720000004.flv")
			case "duplicate-file":
				files[1] = files[0]
			case "missing-generation":
				observations, files = observations[4:], files[2:]
			case "http-failure":
				observations[0].Status = 503
			}
			counts, err := validateRecordingTargetCallbacks(observations, files, 1720000000000)
			if scenario == "valid" {
				if err != nil || len(counts) != 20 {
					test.Fatalf("valid matrix rejected: counts=%v err=%v", counts, err)
				}
				for key, count := range counts {
					if count != 1 || (!strings.HasSuffix(key, "/OnRecord") && !strings.HasSuffix(key, "/OnRecordDone")) {
						test.Fatalf("unexpected callback count %s=%d", key, count)
					}
				}
			} else if err == nil {
				test.Fatal("incomplete or contradictory callback evidence accepted")
			}
		})
	}
}
