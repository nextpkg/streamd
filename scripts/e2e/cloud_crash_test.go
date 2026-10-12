package main

import (
	"encoding/json"
	"fmt"
	"path"
	"testing"

	"github.com/nextpkg/streamd/internal/objectstore"
)

func TestCloudCrashEvidenceRequiresPersistedState(test *testing.T) {
	for _, mode := range []string{"confirmed", "missing", "stale"} {
		for _, mutation := range []string{"valid", "missing-upload", "changed-checkpoint", "changed-upload", "wrong-parts"} {
			test.Run(mode+"/"+mutation, func(test *testing.T) {
				seed := crashCloudState{Uploads: []objectstore.Upload{{Key: "flv/sample/1.flv", ID: "flv-id"}, {Key: "ts/sample/1.ts", ID: "ts-id"}}, Parts: map[string][]objectstore.Part{"flv/sample/1.flv": {}, "ts/sample/1.ts": {}}, Checkpoints: map[string]string{"flv-checkpoint": "confirmed-flv", "ts-checkpoint": "confirmed-ts"}}
				body, err := json.Marshal(seed)
				if err != nil {
					test.Fatal(err)
				}
				var crashed crashCloudState
				if err := json.Unmarshal(body, &crashed); err != nil {
					test.Fatal(err)
				}
				if mode == "missing" {
					crashed.Checkpoints = map[string]string{}
				}
				if mode != "confirmed" {
					for key := range crashed.Parts {
						crashed.Parts[key] = []objectstore.Part{{Number: 1, ETag: "uploaded", Size: objectstore.MinBlockSize}}
					}
				}
				switch mutation {
				case "missing-upload":
					crashed.Uploads = crashed.Uploads[:1]
				case "changed-checkpoint":
					crashed.Checkpoints["flv-checkpoint"] = "changed"
				case "changed-upload":
					if mode == "missing" {
						crashed.Uploads = nil
					} else {
						crashed.Uploads[0].ID = "replacement"
					}
				case "wrong-parts":
					if mode == "confirmed" {
						crashed.Parts["flv/sample/1.flv"] = []objectstore.Part{{Number: 1, ETag: "advanced", Size: objectstore.MinBlockSize}}
					} else {
						crashed.Parts["flv/sample/1.flv"] = nil
					}
				}
				if err := validateCloudCrashState(mode, seed, crashed); (err == nil) != (mutation == "valid") {
					test.Fatalf("unexpected crash-state decision: %v", err)
				}
			})
		}
	}
}

func TestCloudCrashCallbacksRejectFalseCompletionAndReplay(test *testing.T) {
	for _, mode := range []string{"confirmed", "missing", "stale"} {
		for _, mutation := range []string{"valid", "false-completion", "missing-start", "duplicate-publisher", "unknown-file", "missing-file-completion"} {
			test.Run(mode+"/"+mutation, func(test *testing.T) {
				starts := map[string]int{"confirmed": 3, "missing": 1, "stale": 2}[mode]
				observations := []callbackObservation{}
				files := []string{}
				uploads := []objectstore.Upload{}
				add := func(app, generation, event, location string, occur float64) {
					payload := map[string]any{"event": event, "key": app + "/sample", "occur": occur}
					if location != "" {
						payload["location"], payload["filename"], payload["connInfo"] = location, path.Base(location), map[string]any{"app": app}
					}
					observations = append(observations, callbackObservation{Path: "/" + generation, Status: 200, Received: 10000, Payload: payload})
				}
				for _, app := range []string{"flv", "ts"} {
					cloud := fmt.Sprintf("%s/sample/1.%s", app, app)
					uploads = append(uploads, objectstore.Upload{Key: cloud, ID: app + "-id"})
					for index := 0; index < starts; index++ {
						add(app, "publication", "OnPublish", "", float64(1000+index*500))
						add(app, "initial", "OnRecord", "/"+cloud, float64(1010+index*500))
						if index != 0 {
							add(app, "publication", "OnPublishDone", "", float64(1200+index*500))
						}
					}
					add(app, "publication", "OnPublish", "", 5000)
					add(app, "publication", "OnPublishDone", "", 9000)
					location := fmt.Sprintf("/%s/sample/6.%s", app, app)
					files = append(files, "recovered"+location)
					add(app, "recovered", "OnRecord", location, 6000)
					add(app, "recovered", "OnRecordDone", location, 8000)
				}
				switch mutation {
				case "false-completion":
					observations[1].Payload["event"] = "OnRecordDone"
				case "missing-start":
					observations = append(observations[:1], observations[2:]...)
				case "duplicate-publisher":
					observations = append(observations, observations[0])
				case "unknown-file":
					files = append(files, "recovered/flv/sample/7.flv")
				case "missing-file-completion":
					observations = observations[:len(observations)-1]
				}
				if _, err := validateCloudCrashCallbacks(observations, files, mode, 1000, 5000, uploads); (err == nil) != (mutation == "valid") {
					test.Fatalf("unexpected crash callback decision: %v", err)
				}
			})
		}
	}
}
