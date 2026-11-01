package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/nextpkg/streamd/internal/objectstore"
)

func validateCloudCrashCallbacks(observations []callbackObservation, files []string, mode string, started, liveStarted int64, uploads []objectstore.Upload) (map[string]int, error) {
	expectedStarts := map[string]int{"confirmed": 3, "missing": 1, "stale": 2}[mode]
	if expectedStarts == 0 || len(uploads) != 2 {
		return nil, errors.New("invalid crash callback expectations")
	}
	expectedCloud := map[string]bool{}
	for _, upload := range uploads {
		expectedCloud["/"+upload.Key] = true
	}
	counts := map[string]int{}
	live := []callbackObservation{}
	for _, observation := range observations {
		payload := observation.Payload
		event, eventOK := payload["event"].(string)
		key, keyOK := payload["key"].(string)
		occur, timeOK := payload["occur"].(float64)
		if !eventOK || !keyOK || (key != "flv/sample" && key != "ts/sample") || !timeOK || math.IsNaN(occur) || math.IsInf(occur, 0) || math.Trunc(occur) != occur || occur < float64(started) || occur > float64(observation.Received) || observation.Status != http.StatusOK {
			return nil, errors.New("invalid crash callback envelope")
		}
		counts[key+"/"+observation.Path+"/"+event]++
		switch observation.Path {
		case "/publication":
			if len(payload) != 3 || (event != "OnPublish" && event != "OnPublishDone") {
				return nil, errors.New("invalid crash publication event")
			}
			if occur >= float64(liveStarted) {
				live = append(live, observation)
			}
		case "/recovered":
			live = append(live, observation)
		case "/initial":
			location, locationOK := payload["location"].(string)
			connection, connectionOK := payload["connInfo"].(map[string]any)
			app := strings.Split(key, "/")[0]
			if event != "OnRecord" || !locationOK || !expectedCloud[location] || !connectionOK || connection["app"] != app || path.Dir(location) != "/"+key || path.Ext(location) != "."+app || payload["filename"] != path.Base(location) || len(payload) != 6 {
				return nil, errors.New("paused or crashed cloud object falsely completed or changed identity")
			}
		default:
			return nil, errors.New("unknown crash callback generation")
		}
	}
	for _, app := range []string{"flv", "ts"} {
		key := app + "/sample/"
		if counts[key+"/initial/OnRecord"] != expectedStarts || counts[key+"/publication/OnPublish"] != expectedStarts+1 || counts[key+"/publication/OnPublishDone"] != expectedStarts {
			return nil, errors.New("crash lifecycle callbacks missing or duplicated")
		}
	}
	if _, err := validateStorageCallbacks(live, files, nil, liveStarted); err != nil {
		return nil, err
	}
	return counts, nil
}

func (harness *harness) cloudCrashErrors(mode string) error {
	expected := ""
	if mode == "missing" {
		expected = "existing multipart upload has no recording checkpoint; refusing unsafe append"
	}
	if mode == "stale" {
		expected = "recording checkpoint does not match remote upload; refusing unsafe append"
	}
	counts := map[string]int{}
	failures := []string{}
	for _, name := range []string{"server-crashed.log", "server.log"} {
		body, err := os.ReadFile(filepath.Join(harness.evidence, name))
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(bytes.NewReader(body))
		for scanner.Scan() {
			var event struct{ Level, Msg string }
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				return err
			}
			if event.Level != "ERROR" {
				continue
			}
			if name != "server.log" || expected == "" || !strings.Contains(event.Msg, "open failed") || !strings.Contains(event.Msg, expected) {
				return fmt.Errorf("unexpected cloud crash service error: %s", event.Msg)
			}
			failures = append(failures, event.Msg)
			for _, app := range []string{"flv", "ts"} {
				if strings.Contains(event.Msg, "["+app+"/sample]recording failed:") {
					counts[app]++
				}
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	if expected != "" && (counts["flv"] == 0 || counts["ts"] == 0) {
		return errors.New("missing named unsafe-resume rejection")
	}
	harness.results["cloud_crash_expected_errors"] = failures
	return nil
}
