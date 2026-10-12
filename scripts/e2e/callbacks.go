package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/config"
)

type callbackObservation struct {
	Path     string         `json:"path"`
	Received int64          `json:"received_milliseconds"`
	Payload  map[string]any `json:"payload"`
	Status   int            `json:"status"`
}

type callbackReceiver struct {
	mu           sync.Mutex
	observations []callbackObservation
	fail         bool
	stopping     bool
	cancelled    int
	premature    int
	invalid      string
}

func (receiver *callbackReceiver) serve(response http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(io.LimitReader(request.Body, 65537))
	var payload map[string]any
	if err == nil && len(body) <= 65536 {
		err = json.Unmarshal(body, &payload)
	}
	receiver.mu.Lock()
	if err != nil || len(body) > 65536 || request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" {
		receiver.invalid = "invalid callback HTTP envelope"
	}
	status := http.StatusOK
	if receiver.fail {
		status = http.StatusServiceUnavailable
	}
	stalled := payload["event"] == "OnPublishDone"
	if stalled {
		status = 0
	}
	receiver.observations = append(receiver.observations, callbackObservation{Path: request.URL.Path, Received: time.Now().UnixMilli(), Payload: payload, Status: status})
	receiver.mu.Unlock()
	if stalled {
		<-request.Context().Done()
		receiver.mu.Lock()
		receiver.cancelled++
		if !receiver.stopping {
			receiver.premature++
		}
		receiver.mu.Unlock()
		return
	}
	response.WriteHeader(status)
}

func (receiver *callbackReceiver) snapshot() ([]callbackObservation, int, int, string) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	return append([]callbackObservation(nil), receiver.observations...), receiver.cancelled, receiver.premature, receiver.invalid
}

func validateCallbackObservations(observations []callbackObservation, files []string, started int64, directory string) (map[string]int, error) {
	counts := map[string]int{}
	records := map[string]map[string]string{}
	for _, observation := range observations {
		payload := observation.Payload
		event, valid := payload["event"].(string)
		key, keyValid := payload["key"].(string)
		occur, timeValid := payload["occur"].(float64)
		if !valid || !keyValid || !timeValid || (key != "flv/sample" && key != "ts/sample") || occur < float64(started) || occur > float64(observation.Received) || (observation.Path != "/initial" && observation.Path != "/rotated") {
			return nil, errors.New("callback identity/timestamp/target mismatch")
		}
		counts[key+"/"+event]++
		switch event {
		case "OnPublish", "OnPublishDone":
			if len(payload) != 3 || observation.Path != "/initial" || (event == "OnPublish" && observation.Status != http.StatusOK) || (event == "OnPublishDone" && observation.Status != 0) {
				return nil, errors.New("publication callback did not retain its initial snapshot")
			}
		case "OnRecord", "OnRecordDone":
			location, locationValid := payload["location"].(string)
			connection, connectionValid := payload["connInfo"].(map[string]any)
			if len(payload) != 6 || !locationValid || !connectionValid || !strings.HasPrefix(location, "/"+key+"/") || payload["filename"] != path.Base(location) || connection["app"] != strings.Split(key, "/")[0] || (observation.Status != http.StatusOK && observation.Status != http.StatusServiceUnavailable) {
				return nil, errors.New("recording callback payload mismatch")
			}
			if records[location] == nil {
				records[location] = map[string]string{}
			}
			if records[location][event] != "" {
				return nil, fmt.Errorf("duplicate %s callback for %s", event, location)
			}
			records[location][event] = observation.Path
		default:
			return nil, errors.New("unexpected callback event")
		}
	}
	for _, key := range []string{"flv/sample", "ts/sample"} {
		if counts[key+"/OnPublish"] != 1 || counts[key+"/OnPublishDone"] != 1 {
			return nil, errors.New("publication callback count differs from the two real publishers")
		}
	}
	if len(records) != len(files) || len(files) < 20 {
		return nil, errors.New("recording callback/file cardinality mismatch")
	}
	generations := map[string]int{}
	for _, filename := range files {
		relative, err := filepath.Rel(directory, filename)
		if err != nil {
			return nil, err
		}
		pair := records["/"+filepath.ToSlash(relative)]
		if len(pair) != 2 || pair["OnRecord"] == "" || pair["OnRecord"] != pair["OnRecordDone"] {
			return nil, fmt.Errorf("recording callback pair missing or changed target: %s", relative)
		}
		generations[pair["OnRecord"]]++
	}
	if generations["/initial"] < 2 || generations["/rotated"] < 2 {
		return nil, errors.New("recording callback reload not observed")
	}
	return counts, nil
}

func validateCallbackServerErrors(body []byte, rejected, cancelled int) error {
	scanner := bufio.NewScanner(bytes.NewReader(body))
	observedRejected, observedCancelled := 0, 0
	for scanner.Scan() {
		var event struct{ Level, Msg string }
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return err
		}
		if event.Level != "ERROR" && event.Level != "WARN" {
			continue
		}
		if event.Level == "WARN" && event.Msg == "Not continuous timestamp" {
			continue
		}
		matched := false
		for _, key := range []string{"flv/sample", "ts/sample"} {
			if event.Msg == "["+key+"]err=callback request failed,event=OnPublishDone" {
				observedCancelled++
				matched = true
			}
			for _, name := range []string{"OnRecord", "OnRecordDone"} {
				if event.Msg == "["+key+"]err=callback returned HTTP 503,event="+name {
					observedRejected++
					matched = true
				}
			}
		}
		if !matched {
			return fmt.Errorf("unexpected callback scenario server event: %s", event.Msg)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if observedRejected != rejected || observedCancelled != cancelled {
		return fmt.Errorf("callback failures not independently matched: rejected=%d/%d cancelled=%d/%d", observedRejected, rejected, observedCancelled, cancelled)
	}
	return nil
}

func (harness *harness) callbackMedia() error {
	receiver := &callbackReceiver{}
	defer func() {
		observations, cancelled, premature, invalid := receiver.snapshot()
		harness.results["callback_observations"] = observations
		harness.results["callback_receiver_final"] = map[string]any{"cancelled": cancelled, "premature": premature, "invalid": invalid}
	}()
	endpoint := httptest.NewServer(http.HandlerFunc(receiver.serve))
	defer endpoint.Close()
	harness.recordingApplications("immediate", 4, 4)
	configure := func(suffix string) {
		for index := range harness.settings.Application {
			app := &harness.settings.Application[index]
			app.Event = &config.AppEvent{OnPublish: endpoint.URL + suffix, OnPublishDone: endpoint.URL + suffix}
			app.Recorder.Event = &config.RecorderEvent{OnRecord: endpoint.URL + suffix, OnRecordDone: endpoint.URL + suffix}
		}
	}
	configure("/initial")
	started := time.Now()
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", "regular", 60)
	if err != nil {
		return err
	}
	if _, err := harness.waitPlaylist("flv"); err != nil {
		return err
	}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		group, err := harness.players(app, app+"-", 40)
		if err != nil {
			return err
		}
		maps.Copy(players, group)
	}
	watch := harness.watchPlayback(players)
	defer func() { _ = watch.finish(harness) }()
	if err := harness.sleep(6 * time.Second); err != nil {
		return err
	}
	receiver.mu.Lock()
	receiver.fail = true
	receiver.mu.Unlock()
	if err := harness.sleep(8 * time.Second); err != nil {
		return err
	}
	receiver.mu.Lock()
	receiver.fail = false
	receiver.mu.Unlock()
	configure("/rotated")
	if err := harness.commit("rotate-recording-callbacks-with-live-decoders", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
		return err
	}
	if err := harness.finishPlayers(players, 975); err != nil {
		return err
	}
	if err := watch.finish(harness); err != nil {
		return err
	}
	for app, publisher := range publishers {
		if err := harness.finish("publish-"+app, publisher, 70*time.Second); err != nil {
			return err
		}
	}
	var counts map[string]int
	if err := harness.until("complete callback/file pairs and two pending publication callbacks", 1500*time.Millisecond, func() (bool, error) {
		observations, cancelled, premature, invalid := receiver.snapshot()
		if invalid != "" || cancelled != 0 || premature != 0 {
			return false, errors.New("callback receiver failed or pending callback expired before shutdown")
		}
		files, err := recordingFiles(filepath.Join(harness.evidence, "recordings"))
		if err != nil {
			return false, err
		}
		counts, err = validateCallbackObservations(observations, files, started.UnixMilli(), filepath.Join(harness.evidence, "recordings"))
		return err == nil, nil
	}); err != nil {
		return err
	}
	receiver.mu.Lock()
	receiver.stopping = true
	receiver.mu.Unlock()
	shutdownStarted := time.Now()
	if err := harness.stopServer(); err != nil {
		return err
	}
	if time.Since(shutdownStarted) >= 3*time.Second {
		return errors.New("callbacks did not retire within the shutdown budget")
	}
	observations, cancelled, premature, invalid := receiver.snapshot()
	if cancelled != 2 || premature != 0 || invalid != "" {
		return fmt.Errorf("callback shutdown incomplete: cancelled=%d premature=%d invalid=%s", cancelled, premature, invalid)
	}
	rejected := 0
	for _, observation := range observations {
		if observation.Status == http.StatusServiceUnavailable {
			rejected++
		}
	}
	if rejected < 4 {
		return errors.New("callback failure isolation was not exercised")
	}
	logBody, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	if err := validateCallbackServerErrors(logBody, rejected, cancelled); err != nil {
		return err
	}
	var warnings []string
	for _, line := range strings.Split(strings.TrimSpace(string(logBody)), "\n") {
		var event struct{ Level, Msg string }
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return err
		}
		if event.Level == "WARN" {
			warnings = append(warnings, event.Msg)
		}
	}
	harness.results["preexisting_fragment_warnings"] = warnings
	harness.results["timestamp_warning_boundary"] = "The original Fragment warning also occurs in the preceding stream baseline. It is retained, not certified as timestamp correctness by this callback-specific matrix; complete input/output timestamp comparison remains an open gate. Unknown warnings or any unmatched error still fail."
	harness.results["callback_observations"] = observations
	harness.results["callback_counts"] = counts
	harness.results["callback_rejected_responses"] = rejected
	harness.results["callback_shutdown"] = map[string]any{"pending_before_shutdown": 2, "cancelled_after_shutdown": cancelled, "premature_cancellations": premature, "seconds": time.Since(shutdownStarted).Seconds()}
	if err := harness.checkLogs("publish-*.log", "play-*.log"); err != nil {
		return err
	}
	if err := harness.recordings("recordings", 20, true); err != nil {
		return err
	}
	harness.results["callback_media_verified"] = true
	return nil
}
