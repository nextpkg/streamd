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
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type completionFault struct {
	server    *httptest.Server
	enabled   atomic.Bool
	requests  atomic.Int64
	cancelled atomic.Int64
	paths     sync.Map
	release   chan struct{}
}

func newCompletionFault(endpoint string) (*completionFault, error) {
	target, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	fault := &completionFault{release: make(chan struct{})}
	proxy := httputil.NewSingleHostReverseProxy(target)
	fault.server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if fault.enabled.Load() && request.Method == http.MethodPost && request.URL.Query().Get("uploadId") != "" {
			if _, err := io.Copy(io.Discard, request.Body); err != nil {
				return
			}
			fault.paths.Store(request.URL.Path, true)
			fault.requests.Add(1)
			select {
			case <-request.Context().Done():
				fault.cancelled.Add(1)
			case <-fault.release:
				http.Error(response, "injected finalization timeout", http.StatusServiceUnavailable)
			}
			return
		}
		proxy.ServeHTTP(response, request)
	}))
	return fault, nil
}

func (fault *completionFault) close() {
	close(fault.release)
	fault.server.Close()
}

func (harness *harness) storageFault() (result error) {
	faultApp, _, err := storageFaultApplications()
	if err != nil {
		return err
	}
	backend, err := harness.startStorageFaultBackend()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, backend.close(harness)) }()
	fault, err := newCompletionFault(backend.endpoint)
	if err != nil {
		return err
	}
	defer fault.close()
	harness.recordingApplications("immediate", 4, 2)
	recorder := &harness.settings.Application[0].Recorder
	if faultApp == "ts" {
		recorder = &harness.settings.Application[1].Recorder
	}
	backend.configure(recorder, fault.server.URL)
	callbacks := harness.storageCallbacks()
	defer callbacks.server.Close()
	harness.results["fault_application"] = faultApp
	harness.results["changes"] = []map[string]any{}
	harness.results["fault_scope"] = "CompleteMultipartUpload response timeout through a Go HTTP proxy; public SDK/backend identity recorded separately; not vendor outage or disk/quota certification"
	if err := harness.start(); err != nil {
		return err
	}
	defer harness.cleanup()
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
	}
	players := make(map[string]*process)
	for _, app := range []string{"flv", "ts"} {
		group, err := harness.players(app, app+"-", 46)
		if err != nil {
			return err
		}
		maps.Copy(players, group)
	}
	watch := harness.watchPlayback(players)
	defer func() { result = errors.Join(result, watch.finish(harness)) }()
	if err := watch.waitReady(harness.ctx); err != nil {
		return fmt.Errorf("playback warmup: %w", err)
	}
	if err := harness.until("cloud recording active", 5*time.Second, func() (bool, error) {
		report, err := harness.configuration()
		return componentsApplied(report, report.Revision, map[componentKey]string{{"recording", faultApp + "/sample"}: "active"}), err
	}); err != nil {
		return err
	}
	fault.enabled.Store(true)
	harness.results["fault_started_seconds"] = time.Since(harness.started).Seconds()
	recorder.Enable = false
	if err := harness.commit("disable while cloud completion is stalled", map[componentKey]string{{"recording", faultApp + "/sample"}: "disabled"}); err != nil {
		return err
	}
	if err := harness.until("injected completion timeout reached", 5*time.Second, func() (bool, error) { return fault.requests.Load() > 0, nil }); err != nil {
		return err
	}
	*recorder = recorderConfig{Enable: true, Driver: "fs", SaveAs: faultApp, SaveMode: "immediate", Segment: 4, SaveTo: filepath.Join(harness.evidence, "recovered"), Event: callbacks.recordingEvent("recovered")}
	if err := harness.commit("recover to filesystem while old cloud cleanup remains blocked", map[componentKey]string{{"recording", faultApp + "/sample"}: "active"}); err != nil {
		return err
	}
	var recovered componentStatus
	if err := harness.until("late storage failure preserved without rolling back recovered recording", 35*time.Second, func() (bool, error) {
		report, err := harness.configuration()
		if err != nil {
			return false, err
		}
		for _, state := range report.Components {
			if state.Component == "recording" && state.Stream == faultApp+"/sample" {
				recovered = state
				return state.State == "active" && state.Revision == report.Revision && state.Failures == 1 && state.LastFailureAt > 0, nil
			}
		}
		return false, nil
	}); err != nil {
		return err
	}
	harness.results["recovered_recording_status"] = recovered
	harness.results["injected_completion_requests"] = fault.requests.Load()
	harness.results["cancelled_completion_requests"] = fault.cancelled.Load()
	if fault.requests.Load() == 0 || fault.cancelled.Load() == 0 {
		return errors.New("storage timeout was not exercised")
	}
	if err := harness.finishPlayers(players, 46*25*9/10); err != nil {
		return err
	}
	for name, publisher := range publishers {
		if err := harness.finish("publish-"+name, publisher, 75*time.Second); err != nil {
			return err
		}
	}
	if err := callbacks.wait(harness, faultApp, &fault.paths); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	if err := harness.recordings("recordings", 10, true); err != nil {
		return err
	}
	if err := harness.recordings("recovered", 8, true); err != nil {
		return err
	}
	if err := callbacks.verify(harness, faultApp, &fault.paths); err != nil {
		return err
	}
	pending, keys, err := backend.inventory(harness)
	if err != nil {
		return err
	}
	if pending != 1 {
		return fmt.Errorf("failed completion should preserve exactly one upload, got %d", pending)
	}
	harness.results["failed_cloud_upload"] = map[string]any{"pending_uploads": pending, "object_keys": keys, "cleanup": "removed with task-only backend; failed cloud object recovery remains a separate gate"}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	expected, err := finalizationErrorsForProvider(body, faultApp, backend.provider)
	harness.results["expected_server_errors"] = expected
	if err != nil {
		return err
	}
	return harness.checkLogs("publish-*.log", "play-*.log")
}

func finalizationErrors(body []byte) ([]string, error) {
	return finalizationErrorsForApp(body, "flv")
}

func finalizationErrorsForApp(body []byte, app string) ([]string, error) {
	return finalizationErrorsForProvider(body, app, "aws")
}

func finalizationErrorsForProvider(body []byte, app, provider string) ([]string, error) {
	var expected []string
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		var event struct{ Level, Msg string }
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return expected, fmt.Errorf("invalid server log: %w", err)
		}
		if event.Level != "ERROR" {
			continue
		}
		prefix := strings.HasPrefix(event.Msg, "close failed,sync=true,block=true:") || strings.HasPrefix(event.Msg, "["+app+"/sample]recording configuration finalization: close failed,sync=true,block=true:")
		timeout := strings.Contains(event.Msg, "timeout awaiting response headers") || strings.Contains(event.Msg, "context deadline exceeded")
		operation := provider == "aws" && strings.Contains(event.Msg, "operation error S3: CompleteMultipartUpload")
		if provider == "cos" {
			operation = cosFaultOperation(event.Msg, "Post", "POST")
		}
		if !prefix || !timeout || !operation {
			return expected, fmt.Errorf("unexpected server error: %s", event.Msg)
		}
		expected = append(expected, event.Msg)
	}
	if err := scanner.Err(); err != nil {
		return expected, err
	}
	if len(expected) != 2 {
		return expected, fmt.Errorf("expected storage and recorder failure logs, got %d", len(expected))
	}
	return expected, nil
}
