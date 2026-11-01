package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func (harness *harness) rotationMedia() (result error) {
	backend, err := harness.startStorageFaultBackend()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, backend.close(harness)) }()
	address, err := url.Parse(backend.endpoint)
	if err != nil {
		return err
	}
	proxy := httputil.NewSingleHostReverseProxy(address)
	client := &http.Client{Timeout: 10 * time.Second, Transport: http.DefaultTransport.(*http.Transport).Clone()}
	defer client.CloseIdleConnections()
	gate := make(chan struct{})
	var releaseOnce sync.Once
	var held sync.Map
	var statusMu sync.Mutex
	upstreamStatuses := map[string][]int{}
	defer func() {
		statusMu.Lock()
		defer statusMu.Unlock()
		harness.results["rotation_upstream_statuses"] = upstreamStatuses
	}()
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	endpoint := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost && request.URL.Query().Get("uploadId") != "" {
			body, err := io.ReadAll(io.LimitReader(request.Body, 65537))
			if err != nil || len(body) > 65536 {
				http.Error(response, "invalid completion manifest", http.StatusBadRequest)
				return
			}
			held.Store(request.URL.Path, true)
			response.Header().Set("Content-Type", "application/xml")
			response.WriteHeader(http.StatusOK)
			if err := http.NewResponseController(response).Flush(); err != nil {
				return
			}
			select {
			case <-gate:
			case <-request.Context().Done():
				return
			}
			if request.Context().Err() != nil {
				return
			}
			completed, err := forwardRotationCompletion(client, backend.endpoint, request, body)
			if err != nil {
				statusMu.Lock()
				upstreamStatuses[request.URL.Path] = append(upstreamStatuses[request.URL.Path], 0)
				statusMu.Unlock()
				return
			}
			defer func() { _ = completed.Body.Close() }()
			statusMu.Lock()
			upstreamStatuses[request.URL.Path] = append(upstreamStatuses[request.URL.Path], completed.StatusCode)
			statusMu.Unlock()
			_, _ = io.Copy(response, completed.Body)
			return
		}
		proxy.ServeHTTP(response, request)
	}))
	defer endpoint.Close()
	defer release()
	harness.recordingApplications("immediate", 4, 2)
	for index := range harness.settings.Application {
		recorder := &harness.settings.Application[index].Recorder
		backend.configure(recorder, endpoint.URL)
		recorder.SaveMode, recorder.Segment = "immediate", 4
	}
	callbacks := harness.storageCallbacks()
	defer callbacks.server.Close()
	defer func() {
		observations, invalid := callbacks.receiver.snapshot()
		harness.results["rotation_callbacks"], harness.results["rotation_invalid_callback"] = observations, invalid
	}()
	harness.results["changes"] = []map[string]any{}
	if err := harness.start(); err != nil {
		return err
	}
	defer harness.cleanup()
	defer release()
	started := time.Now().UnixMilli()
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	players, err := harness.playersForApps(46)
	if err != nil {
		return err
	}
	watch := harness.watchPlayback(players)
	defer func() { result = errors.Join(result, watch.finish(harness)) }()
	if err := watch.waitReady(harness.ctx); err != nil {
		return fmt.Errorf("rotation playback warmup: %w", err)
	}
	if err := harness.until("four held cloud objects per stream and visible bounded wait", 20*time.Second, func() (bool, error) {
		pending, objects, err := backend.inventory(harness)
		if err != nil {
			return false, err
		}
		if pending > 8 || len(objects) != 0 {
			return false, errors.New("rotation exceeded its bound or completed through a closed gate")
		}
		report, err := harness.configuration()
		ready := pending == 8 && componentsApplied(report, report.Revision, map[componentKey]string{{"recording", "flv/sample"}: "waiting_finalization", {"recording", "ts/sample"}: "waiting_finalization"})
		if ready {
			harness.results["rotation_waiting_status"] = report.Components
			harness.results["rotation_pending_at_capacity"] = pending
		}
		return ready, err
	}); err != nil {
		return err
	}
	observations, invalid := callbacks.receiver.snapshot()
	if invalid != "" {
		return errors.New(invalid)
	}
	for _, observation := range observations {
		if observation.Payload["event"] == "OnRecordDone" {
			return errors.New("cloud success callback preceded actual completion")
		}
	}
	heldPaths := []string{}
	held.Range(func(key, value any) bool { heldPaths = append(heldPaths, key.(string)); return true })
	if len(heldPaths) != 8 {
		return fmt.Errorf("expected eight distinct blocked completion objects, got %d", len(heldPaths))
	}
	harness.results["rotation_held_objects"] = heldPaths
	harness.results["rotation_no_premature_completion"] = true
	for index := range harness.settings.Application {
		app := &harness.settings.Application[index]
		app.Recorder = recorderConfig{Enable: true, Driver: "fs", SaveAs: app.AppName, SaveMode: "immediate", Segment: 4, SaveTo: filepath.Join(harness.evidence, "recovered"), Event: callbacks.recordingEvent("recovered")}
	}
	if err := harness.commit("recover filesystem recording while ordinary cloud rotations remain blocked", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
		return err
	}
	harness.results["rotation_recovered_before_release"] = true
	release()
	if err := harness.until("all old cloud rotations completed", 10*time.Second, func() (bool, error) {
		statusMu.Lock()
		var upstreamError error
		for _, statuses := range upstreamStatuses {
			for _, status := range statuses {
				if status != http.StatusOK {
					upstreamError = fmt.Errorf("completion gate upstream returned HTTP %d", status)
				}
			}
		}
		statusMu.Unlock()
		if upstreamError != nil {
			return false, upstreamError
		}
		pending, keys, err := backend.inventory(harness)
		return pending == 0 && len(keys) == 8, err
	}); err != nil {
		return err
	}
	inventory, err := harness.roundExpiryInventory(backend.reader, backend.contract, nil, true)
	if err != nil || len(inventory) != 8 {
		return fmt.Errorf("rotation cloud inventory: %d objects, %v", len(inventory), err)
	}
	harness.results["rotation_cloud_files"] = inventory
	if err := harness.finishFaultMedia(players, publishers, func() error {
		return harness.until("rotation callbacks after natural recording retirement", 5*time.Second, func() (bool, error) {
			report, err := harness.configuration()
			if err != nil {
				return false, err
			}
			for _, component := range report.Components {
				if component.Component == "recording" {
					return false, nil
				}
			}
			files, err := harness.rotationCallbackFiles()
			if err != nil {
				return false, err
			}
			observations, invalid := callbacks.receiver.snapshot()
			if invalid != "" {
				return false, errors.New(invalid)
			}
			_, err = validateStorageCallbacks(observations, files, nil, started)
			return err == nil, nil
		})
	}); err != nil {
		return err
	}
	if err := harness.recordings("recordings", 8, true); err != nil {
		return err
	}
	if err := harness.recordings("recovered", 12, true); err != nil {
		return err
	}
	files, err := harness.rotationCallbackFiles()
	if err != nil {
		return err
	}
	observations, invalid = callbacks.receiver.snapshot()
	if invalid != "" {
		return errors.New(invalid)
	}
	counts, err := validateStorageCallbacks(observations, files, nil, started)
	if err != nil {
		return err
	}
	harness.results["rotation_callback_files"], harness.results["rotation_callback_counts"] = files, counts
	if err := harness.cloudServerErrors(); err != nil {
		return err
	}
	if err := harness.checkLogs("publish-*.log", "play-*.log"); err != nil {
		return err
	}
	harness.results["rotation_verified"] = true
	return nil
}

func forwardRotationCompletion(client *http.Client, endpoint string, request *http.Request, body []byte) (*http.Response, error) {
	upstream, err := http.NewRequestWithContext(request.Context(), http.MethodPost, endpoint+request.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	upstream.Header = request.Header.Clone()
	upstream.Host = request.Host
	return client.Do(upstream)
}

func (harness *harness) rotationCallbackFiles() ([]string, error) {
	files := []string{}
	for directory, generation := range map[string]string{"recordings": "initial", "recovered": "recovered"} {
		paths, err := recordingFiles(filepath.Join(harness.evidence, directory))
		if err != nil {
			return nil, err
		}
		for _, filename := range paths {
			relative, err := filepath.Rel(filepath.Join(harness.evidence, directory), filename)
			if err != nil {
				return nil, err
			}
			files = append(files, generation+"/"+filepath.ToSlash(relative))
		}
	}
	for _, filename := range files {
		if !strings.HasPrefix(filename, "initial/") && !strings.HasPrefix(filename, "recovered/") {
			return nil, errors.New("unknown rotation generation")
		}
	}
	return files, nil
}
