package main

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/config"
)

type storageFaultCallbacks struct {
	receiver recordingTargetCallbacks
	server   *httptest.Server
}

func storageFaultApplications() (string, string, error) {
	switch app := os.Getenv("STREAMD_E2E_FAULT_APP"); app {
	case "", "flv":
		return "flv", "ts", nil
	case "ts":
		return "ts", "flv", nil
	default:
		return "", "", fmt.Errorf("unsupported storage fault application %q", app)
	}
}

func (harness *harness) storageCallbacks() *storageFaultCallbacks {
	callbacks := &storageFaultCallbacks{}
	callbacks.server = httptest.NewServer(http.HandlerFunc(callbacks.receiver.serve))
	for index := range harness.settings.Application {
		app := &harness.settings.Application[index]
		endpoint := callbacks.server.URL + "/publication"
		app.Event = &config.AppEvent{OnPublish: endpoint, OnPublishDone: endpoint}
		app.Recorder.Event = callbacks.recordingEvent("initial")
	}
	return callbacks
}

func (callbacks *storageFaultCallbacks) recordingEvent(generation string) *config.RecorderEvent {
	endpoint := callbacks.server.URL + "/" + generation
	return &config.RecorderEvent{OnRecord: endpoint, OnRecordDone: endpoint}
}

func (callbacks *storageFaultCallbacks) verify(harness *harness, faultApp string, faultPaths *sync.Map) error {
	files := []string{}
	for directory, generation := range map[string]string{"recordings": "initial", "recovered": "recovered"} {
		root := filepath.Join(harness.evidence, directory)
		if err := filepath.WalkDir(root, func(filename string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			relative, err := filepath.Rel(root, filename)
			if err != nil {
				return err
			}
			files = append(files, generation+"/"+filepath.ToSlash(relative))
			return nil
		}); err != nil {
			return err
		}
	}
	sort.Strings(files)
	failed := []string{}
	faultPaths.Range(func(key, value any) bool {
		failed = append(failed, strings.TrimPrefix(key.(string), "/test-bucket"))
		return true
	})
	if len(failed) != 1 || !strings.HasPrefix(failed[0], "/"+faultApp+"/sample/") || path.Ext(failed[0]) != "."+faultApp {
		return fmt.Errorf("faulted object identity mismatch: %v", failed)
	}
	observations, invalid := callbacks.receiver.snapshot()
	if invalid != "" {
		return errors.New(invalid)
	}
	counts, err := validateStorageFaultCallbacks(observations, files, failed[0], harness.started.UnixMilli())
	if err != nil {
		return err
	}
	harness.results["storage_callback_counts"] = counts
	harness.results["storage_callback_observations"] = observations
	harness.results["storage_callback_completed_files"] = files
	harness.results["storage_callback_failed_object"] = failed[0]
	harness.results["storage_callbacks_verified"] = true
	return nil
}

func (callbacks *storageFaultCallbacks) wait(harness *harness, faultApp string, faultPaths *sync.Map) error {
	var lastError error
	err := harness.until("natural storage callbacks agree with completed files and failed object", 5*time.Second, func() (bool, error) {
		lastError = callbacks.verify(harness, faultApp, faultPaths)
		return lastError == nil, nil
	})
	return errors.Join(err, lastError)
}

func validateStorageFaultCallbacks(observations []callbackObservation, files []string, failed string, started int64) (map[string]int, error) {
	if failed == "" {
		return nil, errors.New("missing independent fault evidence")
	}
	return validateStorageCallbacks(observations, files, []string{"initial" + failed}, started)
}

func validateStorageCallbacks(observations []callbackObservation, files, failed []string, started int64) (map[string]int, error) {
	expected := map[string]bool{}
	for _, filename := range failed {
		if _, exists := expected[filename]; exists || filename == "" {
			return nil, errors.New("duplicate or empty failed object")
		}
		expected[filename] = false
	}
	for _, filename := range files {
		if _, exists := expected[filename]; exists {
			return nil, errors.New("duplicate or failed completed file")
		}
		expected[filename] = true
	}
	if len(files) < 2 {
		return nil, errors.New("missing independent file/fault evidence")
	}
	counts := map[string]int{}
	paired := map[string]map[string]float64{}
	for _, observation := range observations {
		payload := observation.Payload
		event, eventOK := payload["event"].(string)
		key, keyOK := payload["key"].(string)
		occur, timeOK := payload["occur"].(float64)
		if !eventOK || !keyOK || (key != "flv/sample" && key != "ts/sample") || !timeOK || occur < float64(started) || occur > float64(observation.Received) || observation.Status != http.StatusOK {
			return nil, errors.New("storage callback identity, time or HTTP status mismatch")
		}
		counts[key+"/"+event]++
		switch event {
		case "OnPublish", "OnPublishDone":
			if observation.Path != "/publication" || len(payload) != 3 {
				return nil, errors.New("publication callback snapshot changed")
			}
		case "OnRecord", "OnRecordDone":
			location, locationOK := payload["location"].(string)
			connection, connectionOK := payload["connInfo"].(map[string]any)
			app, _, _ := strings.Cut(key, "/")
			if len(payload) != 6 || !locationOK || !connectionOK || !strings.HasPrefix(location, "/"+key+"/") || path.Ext(location) != "."+app || payload["filename"] != path.Base(location) || connection["app"] != app {
				return nil, errors.New("recording callback fields mismatch")
			}
			identity := strings.TrimPrefix(observation.Path, "/") + location
			complete, exists := expected[identity]
			if !exists || (!complete && event == "OnRecordDone") {
				return nil, errors.New("unknown recording or false success for failed object")
			}
			if paired[identity] == nil {
				paired[identity] = map[string]float64{}
			}
			if paired[identity][event] != 0 {
				return nil, errors.New("duplicate recording callback")
			}
			paired[identity][event] = occur
		default:
			return nil, errors.New("unknown callback event")
		}
	}
	for _, app := range []string{"flv", "ts"} {
		if counts[app+"/sample/OnPublish"] != 1 || counts[app+"/sample/OnPublishDone"] != 1 {
			return nil, errors.New("publication callbacks missing or duplicated")
		}
	}
	for filename, complete := range expected {
		pair := paired[filename]
		if pair["OnRecord"] == 0 || (complete && pair["OnRecordDone"] < pair["OnRecord"]) {
			return nil, fmt.Errorf("recording callbacks missing or reversed: %s", filename)
		}
	}
	return counts, nil
}
