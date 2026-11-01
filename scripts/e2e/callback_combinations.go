package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/config"
)

type combinationReceiver struct {
	mu            sync.Mutex
	observations  []callbackObservation
	invalid       string
	release       chan struct{}
	releaseOnce   sync.Once
	activeBurst   int
	shutdownHolds int
	stopping      bool
	premature     int
	cancelled     []string
}

func (receiver *combinationReceiver) releaseBurst() {
	receiver.releaseOnce.Do(func() { close(receiver.release) })
}

func (receiver *combinationReceiver) serve(response http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(io.LimitReader(request.Body, 65537))
	var payload map[string]any
	if err == nil {
		err = json.Unmarshal(body, &payload)
	}
	receiver.mu.Lock()
	if err != nil || len(body) > 65536 || request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" {
		receiver.invalid = "invalid callback envelope"
	}
	key, _ := payload["key"].(string)
	event, _ := payload["event"].(string)
	burst := strings.HasPrefix(key, "pressure/burst-")
	hold := request.URL.Path == "/hold" && key != "pressure/recovered"
	terminal := request.URL.Path == "/during-stop"
	status := http.StatusOK
	if hold || terminal {
		status = 0
	}
	if burst {
		receiver.activeBurst++
	}
	if hold && !burst {
		receiver.shutdownHolds++
	}
	if terminal && !receiver.stopping {
		receiver.premature++
	}
	index := len(receiver.observations)
	receiver.observations = append(receiver.observations, callbackObservation{Path: request.URL.Path, Received: time.Now().UnixMilli(), Payload: payload, Status: status})
	receiver.mu.Unlock()
	if hold || terminal {
		var release <-chan struct{}
		if burst {
			release = receiver.release
		}
		select {
		case <-release:
			receiver.mu.Lock()
			receiver.observations[index].Status = http.StatusOK
			receiver.mu.Unlock()
		case <-request.Context().Done():
			receiver.mu.Lock()
			receiver.cancelled = append(receiver.cancelled, key+"/"+event)
			if !receiver.stopping {
				receiver.premature++
			}
			receiver.mu.Unlock()
		}
	}
	receiver.mu.Lock()
	if burst {
		receiver.activeBurst--
	}
	if hold && !burst {
		receiver.shutdownHolds--
	}
	receiver.mu.Unlock()
	response.WriteHeader(http.StatusOK)
}

func (receiver *combinationReceiver) snapshot() (observations []callbackObservation, state map[string]any) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	return append([]callbackObservation(nil), receiver.observations...), map[string]any{"invalid": receiver.invalid, "active_burst": receiver.activeBurst, "shutdown_holds": receiver.shutdownHolds, "premature": receiver.premature, "cancelled": append([]string(nil), receiver.cancelled...)}
}

func (harness *harness) captureCombinations(receiver *combinationReceiver) {
	observations, state := receiver.snapshot()
	harness.results["callback_observations"] = observations
	harness.results["callback_receiver_final"] = state
}

func (harness *harness) callbackTakeover() error {
	receiver := &combinationReceiver{release: make(chan struct{})}
	endpoint := httptest.NewServer(http.HandlerFunc(receiver.serve))
	defer endpoint.Close()
	defer harness.captureCombinations(receiver)
	return harness.republishWithCallbacks(receiver, endpoint.URL)
}

func (harness *harness) configureTakeoverCallbacks(endpoint string, generation int) {
	for index := range harness.settings.Application {
		app := &harness.settings.Application[index]
		address := fmt.Sprintf("%s/generation/%d", endpoint, generation)
		app.Event = &config.AppEvent{OnPublish: address, OnPublishDone: address}
		app.Recorder.Event = &config.RecorderEvent{OnRecord: endpoint + "/recording", OnRecordDone: endpoint + "/recording"}
	}
}

func (harness *harness) ordinaryCallbackPublications(receiver *combinationReceiver, endpoint string) error {
	harness.settings.Application = append(harness.settings.Application, appConfig{Enable: true, AppName: "ordinary", Event: &config.AppEvent{OnPublish: endpoint + "/ordinary", OnPublishDone: endpoint + "/ordinary"}})
	if err := harness.commit("ordinary-republication-callbacks", nil); err != nil {
		return err
	}
	for generation := range 2 {
		client, err := harness.callbackPublisher("ordinary", "sample")
		if err != nil {
			return err
		}
		defer func() { _ = client.Close() }()
		if err := harness.until("ordinary publication callback", time.Second, func() (bool, error) {
			observations, _ := receiver.snapshot()
			count := 0
			for _, observation := range observations {
				if observation.Payload["key"] == "ordinary/sample" && observation.Payload["event"] == "OnPublish" {
					count++
				}
			}
			return count == generation+1, nil
		}); err != nil {
			return err
		}
		_ = client.Close()
		if err := harness.until("ordinary publisher fully retired before republishing", time.Second, func() (bool, error) {
			observations, _ := receiver.snapshot()
			count := 0
			for _, observation := range observations {
				if observation.Payload["key"] == "ordinary/sample" && observation.Payload["event"] == "OnPublishDone" {
					count++
				}
			}
			publication, err := harness.relayStat("ordinary", "pub")
			return count == generation+1 && publication.Count == 0, err
		}); err != nil {
			return err
		}
	}
	return nil
}

func validateTakeoverCallbacks(observations []callbackObservation, files []string, directory string) (map[string]int, error) {
	counts := map[string]int{}
	records := map[string]map[string]int{}
	for _, observation := range observations {
		payload := observation.Payload
		key, keyValid := payload["key"].(string)
		event, eventValid := payload["event"].(string)
		occur, timeValid := payload["occur"].(float64)
		if !keyValid || !eventValid || !timeValid || occur <= 0 || occur > float64(observation.Received) || (key != "flv/sample" && key != "ts/sample" && key != "ordinary/sample") || observation.Status != http.StatusOK {
			return nil, errors.New("invalid takeover callback identity, time or status")
		}
		counts[key+"/"+event+"/"+observation.Path]++
		if key == "ordinary/sample" {
			if len(payload) != 3 || (event != "OnPublish" && event != "OnPublishDone") || observation.Path != "/ordinary" {
				return nil, errors.New("invalid ordinary republication callback")
			}
			continue
		}
		switch event {
		case "OnPublish", "OnPublishDone":
			if len(payload) != 3 || (observation.Path != "/generation/0" && observation.Path != "/generation/1" && observation.Path != "/generation/2") {
				return nil, errors.New("takeover publication callback snapshot mismatch")
			}
		case "OnRecord", "OnRecordDone":
			location, valid := payload["location"].(string)
			connection, connectionValid := payload["connInfo"].(map[string]any)
			if len(payload) != 6 || !valid || !connectionValid || observation.Path != "/recording" || !strings.HasPrefix(location, "/"+key+"/") || payload["filename"] != path.Base(location) || connection["app"] != strings.Split(key, "/")[0] {
				return nil, errors.New("takeover recording callback mismatch")
			}
			if records[location] == nil {
				records[location] = map[string]int{}
			}
			records[location][event]++
		default:
			return nil, errors.New("unknown takeover callback event")
		}
	}
	for _, key := range []string{"flv/sample", "ts/sample"} {
		for generation := range 3 {
			for _, event := range []string{"OnPublish", "OnPublishDone"} {
				if counts[fmt.Sprintf("%s/%s//generation/%d", key, event, generation)] != 1 {
					return nil, fmt.Errorf("missing or repeated generation %d %s for %s", generation, event, key)
				}
			}
		}
	}
	for _, event := range []string{"OnPublish", "OnPublishDone"} {
		if counts["ordinary/sample/"+event+"//ordinary"] != 2 {
			return nil, errors.New("ordinary republication callback count mismatch")
		}
	}
	if len(records) != len(files) || len(files) == 0 {
		return nil, errors.New("takeover recording callback/file count mismatch")
	}
	for _, filename := range files {
		relative, err := filepath.Rel(directory, filename)
		if err != nil {
			return nil, err
		}
		pair := records["/"+filepath.ToSlash(relative)]
		if len(pair) != 2 || pair["OnRecord"] != 1 || pair["OnRecordDone"] != 1 {
			return nil, fmt.Errorf("takeover did not preserve exactly one callback pair for %s", relative)
		}
	}
	return counts, nil
}
