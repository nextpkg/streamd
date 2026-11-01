package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nextpkg/streamd/config"
)

func (harness *harness) filesystemCallbacks() *storageFaultCallbacks {
	callbacks := &storageFaultCallbacks{}
	callbacks.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == "/ready" {
			_, _ = fmt.Fprint(writer, "ready") // The HTTP client validates the response; disconnects need no retry.
			return
		}
		callbacks.receiver.serve(writer, request)
	}))
	address, _ := url.Parse(callbacks.server.URL)
	callbacks.server.URL = "http://host.lima.internal:" + address.Port()
	for index := range harness.settings.Application {
		app := &harness.settings.Application[index]
		endpoint := callbacks.server.URL + "/publication"
		app.Event = &config.AppEvent{OnPublish: endpoint, OnPublishDone: endpoint}
		app.Recorder.Event = callbacks.recordingEvent("filesystem")
	}
	return callbacks
}

type filesystemClockSample struct {
	BeforeNS int64 `json:"before_host_ns"`
	GuestNS  int64 `json:"guest_ns"`
	AfterNS  int64 `json:"after_host_ns"`
}

func filesystemClockBounds(sample filesystemClockSample) (int64, int64, error) {
	limit := int64(time.Second)
	if sample.AfterNS < sample.BeforeNS || sample.AfterNS-sample.BeforeNS > limit || sample.GuestNS < sample.BeforeNS-limit || sample.GuestNS > sample.AfterNS+limit {
		return 0, 0, errors.New("filesystem clock sample is discontinuous or too uncertain")
	}
	return (sample.GuestNS-sample.AfterNS)/int64(time.Millisecond) - 1, (sample.GuestNS-sample.BeforeNS)/int64(time.Millisecond) + 1, nil
}

func (container *filesystemContainer) sampleClock(harness *harness) (filesystemClockSample, error) {
	ctx, cancel := context.WithTimeout(harness.ctx, 5*time.Second)
	defer cancel()
	sample := filesystemClockSample{BeforeNS: time.Now().UnixNano()}
	body, err := container.runner.command(ctx, "exec", container.runner.name, "/test-clock")
	sample.AfterNS = time.Now().UnixNano()
	if err != nil {
		return sample, err
	}
	sample.GuestNS, err = strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	if err == nil {
		_, _, err = filesystemClockBounds(sample)
	}
	return sample, err
}

func (callbacks *storageFaultCallbacks) verifyFilesystem(harness *harness, faultApp, mode string, messages []string, clockSamples []filesystemClockSample) error {
	files, err := recordingFiles(filepath.Join(harness.evidence, "recordings"))
	if err != nil {
		return err
	}
	identities := []string{}
	for _, filename := range files {
		relative, err := filepath.Rel(filepath.Join(harness.evidence, "recordings"), filename)
		if err != nil {
			return err
		}
		parts := strings.SplitN(filepath.ToSlash(relative), "/", 2)
		if len(parts) != 2 || (parts[0] != "fault" && parts[0] != "healthy") || (strings.HasPrefix(parts[1], faultApp+"/") != (parts[0] == "fault")) {
			return errors.New("recording mount identity mismatch")
		}
		identities = append(identities, "/"+parts[1])
	}
	failed := map[string]bool{}
	pattern := regexp.MustCompile(`/fault/` + regexp.QuoteMeta(faultApp) + `/sample/[0-9]+\.` + regexp.QuoteMeta(faultApp))
	for _, message := range messages {
		filename := pattern.FindString(message)
		if filename == "" || (mode == "permission" && !strings.Contains(message, "open file failed")) || (mode == "full" && !strings.Contains(message, "recording append failed")) {
			return errors.New("filesystem fault lacks a precise recording operation and path")
		}
		failed[strings.TrimPrefix(filename, "/fault")] = mode == "full"
	}
	observations, invalid := callbacks.receiver.snapshot()
	if invalid != "" {
		return errors.New(invalid)
	}
	if len(clockSamples) != 2 {
		return errors.New("filesystem callback clocks were not independently bracketed")
	}
	firstLower, firstUpper, err := filesystemClockBounds(clockSamples[0])
	if err != nil {
		return err
	}
	lastLower, lastUpper, err := filesystemClockBounds(clockSamples[1])
	if err != nil || max(firstLower, lastLower) > min(firstUpper, lastUpper) {
		return errors.New("filesystem host/guest clocks moved outside measured bounds")
	}
	lower, upper := min(firstLower, lastLower), max(firstUpper, lastUpper)
	harness.results["filesystem_clock_samples"] = clockSamples
	harness.results["filesystem_clock_bounds_ms"] = []int64{lower, upper}
	counts, err := validateFilesystemCallbacks(observations, identities, failed, harness.started.UnixMilli(), lower, upper)
	if err != nil {
		return err
	}
	harness.results["filesystem_callback_counts"] = counts
	harness.results["filesystem_callback_files"] = identities
	harness.results["filesystem_callback_failures"] = failed
	harness.results["filesystem_callbacks_verified"] = true
	return nil
}

func validateFilesystemCallbacks(observations []callbackObservation, files []string, failed map[string]bool, started, lowerOffset, upperOffset int64) (map[string]int, error) {
	if len(files) < 2 || len(failed) == 0 {
		return nil, errors.New("missing independent filesystem evidence")
	}
	expected := map[string][2]int{}
	for _, filename := range files {
		if _, exists := expected[filename]; exists {
			return nil, errors.New("duplicate filesystem evidence")
		}
		expected[filename] = [2]int{1, 1}
	}
	for filename, opened := range failed {
		if !opened {
			if _, exists := expected[filename]; exists {
				return nil, errors.New("failed open unexpectedly created a recording")
			}
			expected[filename] = [2]int{}
		} else {
			expected[filename] = [2]int{1, 0}
		}
	}
	actual := map[string][2]int{}
	starts, ends := map[string]float64{}, map[string]float64{}
	counts := map[string]int{}
	for _, observation := range observations {
		payload := observation.Payload
		key, keyOK := payload["key"].(string)
		event, eventOK := payload["event"].(string)
		occur, timeOK := payload["occur"].(float64)
		if !keyOK || (key != "flv/sample" && key != "ts/sample") || !eventOK || !timeOK || math.IsNaN(occur) || math.IsInf(occur, 0) || occur < float64(started+lowerOffset) || occur > float64(observation.Received+upperOffset) || observation.Status != http.StatusOK {
			return nil, errors.New("filesystem callback identity, time or status mismatch")
		}
		counts[key+"/"+event]++
		switch event {
		case "OnPublish", "OnPublishDone":
			if len(payload) != 3 || observation.Path != "/publication" {
				return nil, errors.New("filesystem publisher callback mismatch")
			}
		case "OnRecord", "OnRecordDone":
			location, locationOK := payload["location"].(string)
			connection, connectionOK := payload["connInfo"].(map[string]any)
			app := strings.Split(key, "/")[0]
			if len(payload) != 6 || !locationOK || !connectionOK || connection["app"] != app || observation.Path != "/filesystem" || path.Dir(location) != "/"+key || path.Ext(location) != "."+app || payload["filename"] != path.Base(location) {
				return nil, errors.New("filesystem recording callback mismatch")
			}
			if _, exists := expected[location]; !exists {
				return nil, errors.New("unknown filesystem recording callback")
			}
			pair := actual[location]
			if event == "OnRecord" {
				pair[0]++
				starts[location] = occur
			} else {
				pair[1]++
				ends[location] = occur
			}
			actual[location] = pair
		default:
			return nil, errors.New("unknown filesystem callback event")
		}
	}
	for filename, pair := range expected {
		if actual[filename] != pair || (pair[1] > 0 && ends[filename] < starts[filename]) {
			return nil, fmt.Errorf("filesystem callbacks differ for %s: got %v expected %v", filename, actual[filename], pair)
		}
	}
	for _, app := range []string{"flv", "ts"} {
		if counts[app+"/sample/OnPublish"] != 1 || counts[app+"/sample/OnPublishDone"] != 1 {
			return nil, errors.New("filesystem publisher lifecycle mismatch")
		}
	}
	return counts, nil
}
