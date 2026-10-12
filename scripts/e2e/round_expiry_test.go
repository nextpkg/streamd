package main

import (
	"fmt"
	"math"
	"path"
	"testing"
)

func roundExpiryFixture(finished bool) ([]callbackObservation, []string) {
	observations := []callbackObservation{}
	tails := []string{}
	for _, app := range []string{"flv", "ts"} {
		observations = append(observations, callbackObservation{Path: "/publication", Received: 1720000000100, Status: 200, Payload: map[string]any{"event": "OnPublish", "key": app + "/sample", "occur": float64(1720000000000)}})
		for index := 0; index < 5; index++ {
			bucket := int64(1720000000 + index*8)
			location := fmt.Sprintf("/%s/sample/%d.%s", app, bucket, app)
			for _, event := range []string{"OnRecord", "OnRecordDone"} {
				occur := bucket*1000 + 1000
				if event == "OnRecordDone" {
					if index == 4 && !finished {
						continue
					}
					occur = (bucket+8)*1000 + 100
					if index == 4 {
						occur = bucket*1000 + 3000
					}
				}
				observations = append(observations, callbackObservation{Path: "/round", Received: occur + 50, Status: 200, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(occur), "filename": path.Base(location), "location": location, "connInfo": map[string]any{"app": app}}})
			}
			if index == 4 {
				tails = append(tails, location)
			}
		}
		if finished {
			observations = append(observations, callbackObservation{Path: "/publication", Received: 1720000060050, Status: 200, Payload: map[string]any{"event": "OnPublishDone", "key": app + "/sample", "occur": float64(1720000060000)}})
		}
	}
	return observations, tails
}

func TestRoundExpiryRequiresNaturalBoundaryAndSeparateForcedTails(test *testing.T) {
	for _, scenario := range []string{"live", "final", "early-completion", "duplicate", "missing-start", "bad-bucket", "wrong-connection", "nan", "future", "missing-publication", "wrong-target", "http-failure", "tail-counted-as-natural", "unknown-tail", "duplicate-tail", "extra-active"} {
		test.Run(scenario, func(test *testing.T) {
			finished := scenario != "live" && scenario != "extra-active"
			observations, tails := roundExpiryFixture(finished)
			if !finished {
				tails = nil
			}
			switch scenario {
			case "early-completion":
				observations[2].Payload["occur"] = float64(1720000007000)
			case "duplicate":
				observations = append(observations, observations[1])
			case "missing-start":
				observations = append(observations[:1], observations[2:]...)
			case "bad-bucket":
				observations[1].Payload["location"], observations[1].Payload["filename"] = "/flv/sample/1720000001.flv", "1720000001.flv"
			case "wrong-connection":
				observations[1].Payload["connInfo"] = map[string]any{"app": "ts"}
			case "nan":
				observations[1].Payload["occur"] = math.NaN()
			case "future":
				observations[1].Received = 1720000000000
			case "missing-publication":
				observations = observations[1:]
			case "wrong-target":
				observations[1].Path = "/other"
			case "http-failure":
				observations[1].Status = 503
			case "tail-counted-as-natural":
				tails = nil
			case "unknown-tail":
				tails[0] = "/flv/sample/1720000040.flv"
			case "duplicate-tail":
				tails = append(tails, tails[0])
			case "extra-active":
				observations = append(observations[:2], observations[3:]...)
			}
			state, err := validateRoundExpiry(observations, 1720000000000, tails, finished)
			if scenario == "live" || scenario == "final" {
				if err != nil || len(state.Natural) != 8 || (!finished && len(state.Active) != 2) || (finished && len(state.Active) != 0) {
					test.Fatalf("valid round lifecycle rejected: %+v %v", state, err)
				}
			} else if err == nil {
				test.Fatal("incomplete or contradictory lifecycle accepted")
			}
		})
	}
}

func TestRoundExpiryRequiresIndependentFileIdentity(test *testing.T) {
	inventory := map[string]string{"/flv/sample/1720000000.flv": "digest-one", "/ts/sample/1720000000.ts": "digest-two"}
	valid := []string{"/flv/sample/1720000000.flv", "/ts/sample/1720000000.ts"}
	if !sameRoundFiles(inventory, valid) {
		test.Fatal("matching independently listed files rejected")
	}
	for _, invalid := range [][]string{valid[:1], {valid[0], valid[0]}, {valid[0], "/ts/sample/1720000008.ts"}} {
		if sameRoundFiles(inventory, invalid) {
			test.Fatal("missing, duplicated or unexpected recording accepted")
		}
	}
}
