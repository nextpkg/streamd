package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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

type uploadFault struct {
	server    *httptest.Server
	mode      string
	enabled   atomic.Bool
	requests  atomic.Int64
	cancelled atomic.Int64
	resets    atomic.Int64
	paths     sync.Map
	release   chan struct{}
}

func newUploadFault(endpoint, mode string) (*uploadFault, error) {
	if mode != "queue" && mode != "timeout" && mode != "reset" && mode != "quota" {
		return nil, fmt.Errorf("unsupported upload fault %q", mode)
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	fault := &uploadFault{mode: mode, release: make(chan struct{})}
	proxy := httputil.NewSingleHostReverseProxy(target)
	fault.server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if !fault.enabled.Load() || request.Method != http.MethodPut || request.URL.Query().Get("uploadId") == "" || request.URL.Query().Get("partNumber") == "" {
			proxy.ServeHTTP(response, request)
			return
		}
		if _, err := io.Copy(io.Discard, request.Body); err != nil {
			return
		}
		fault.paths.Store(request.URL.Path, true)
		fault.requests.Add(1)
		switch mode {
		case "queue", "timeout":
			select {
			case <-request.Context().Done():
				fault.cancelled.Add(1)
			case <-fault.release:
				http.Error(response, "injected upload timeout", http.StatusServiceUnavailable)
			}
		case "reset":
			connection, _, err := response.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			if tcp, ok := connection.(*net.TCPConn); ok {
				_ = tcp.SetLinger(0)
			}
			_ = connection.Close()
			fault.resets.Add(1)
		case "quota":
			response.Header().Set("Content-Type", "application/xml")
			response.WriteHeader(http.StatusInsufficientStorage)
			_, _ = io.WriteString(response, "<Error><Code>InsufficientStorage</Code><Message>injected capacity exhaustion</Message></Error>")
		}
	}))
	return fault, nil
}

func (fault *uploadFault) close() { close(fault.release); fault.server.Close() }

func (harness *harness) faultPlayers() (map[string]*process, *playbackWatch, error) {
	players, err := harness.playersForApps(46)
	if err != nil {
		return nil, nil, err
	}
	watch := harness.watchPlayback(players)
	select {
	case <-watch.ready:
		return players, watch, nil
	case <-watch.done:
		return players, watch, fmt.Errorf("playback warmup: %w", watch.err)
	case <-harness.ctx.Done():
		return players, watch, harness.ctx.Err()
	}
}

func (harness *harness) recordingStatus(app string) (componentStatus, error) {
	report, err := harness.configuration()
	if err != nil {
		return componentStatus{}, err
	}
	for _, state := range report.Components {
		if state.Component == "recording" && state.Stream == app+"/sample" {
			return state, nil
		}
	}
	return componentStatus{}, nil
}

func (harness *harness) waitRecording(label, app string, timeout time.Duration, predicate func(componentStatus) bool) (componentStatus, error) {
	var state componentStatus
	err := harness.until(label, timeout, func() (bool, error) {
		var err error
		state, err = harness.recordingStatus(app)
		return predicate(state), err
	})
	return state, err
}

func faultErrors(body []byte, matches func(string) bool) ([]string, error) {
	return faultErrorsForApp(body, "flv", matches)
}

func faultErrorsForApp(body []byte, app string, matches func(string) bool) ([]string, error) {
	var expected []string
	recorderReported := false
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		var event struct{ Level, Msg string }
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return expected, err
		}
		if event.Level != "ERROR" {
			continue
		}
		if !matches(event.Msg) {
			return expected, fmt.Errorf("unexpected server error: %s", event.Msg)
		}
		recorderReported = recorderReported || strings.Contains(event.Msg, "["+app+"/sample]recording failed:")
		expected = append(expected, event.Msg)
	}
	if err := scanner.Err(); err != nil {
		return expected, err
	}
	if !recorderReported {
		return expected, errors.New("missing recording failure log")
	}
	return expected, nil
}

func uploadErrorMatches(mode, message string) bool {
	if mode == "queue" {
		return strings.Contains(message, "recording upload queue capacity exceeded")
	}
	if !strings.Contains(message, "operation error S3: UploadPart") {
		return false
	}
	switch mode {
	case "timeout":
		return strings.Contains(message, "timeout awaiting response headers") || strings.Contains(message, "context deadline exceeded")
	case "reset":
		return strings.Contains(message, "connection reset by peer") || strings.Contains(message, "EOF") || strings.Contains(message, "broken pipe")
	case "quota":
		return strings.Contains(message, "InsufficientStorage") && strings.Contains(message, "injected capacity exhaustion")
	}
	return false
}

func (harness *harness) finishFaultMedia(players, publishers map[string]*process, beforeStop ...func() error) error {
	if err := harness.finishPlayers(players, 46*25*9/10); err != nil {
		return err
	}
	for app, publisher := range publishers {
		if err := harness.finish("publish-"+app, publisher, 75*time.Second); err != nil {
			return err
		}
	}
	for _, check := range beforeStop {
		if err := check(); err != nil {
			return err
		}
	}
	return harness.stopServer()
}

func (harness *harness) uploadFault() (result error) {
	faultApp, healthyApp, err := storageFaultApplications()
	if err != nil {
		return err
	}
	mode := os.Getenv("STREAMD_E2E_FAULT")
	if mode == "" {
		mode = "queue"
	}
	backend, err := harness.startStorageFaultBackend()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, backend.close(harness)) }()
	fault, err := newUploadFault(backend.endpoint, mode)
	if err != nil {
		return err
	}
	defer fault.close()
	bitrate := "12M"
	if mode == "queue" {
		bitrate = "32M"
	}
	if mode == "timeout" {
		bitrate = "4M"
	}
	source := filepath.Join(harness.evidence, "upload-source.flv")
	fixture, err := harness.launch("fixture", "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-i", filepath.Join(harness.root, "testdata/generated/bframes.flv"), "-t", "5",
		"-vf", "scale=1280:720,noise=alls=24:allf=t+u:all_seed=1", "-c:a", "copy", "-c:v", "libx264", "-preset", "ultrafast", "-g", "25", "-bf", "2",
		"-b:v", bitrate, "-minrate", bitrate, "-maxrate", bitrate, "-bufsize", bitrate, "-x264-params", "nal-hrd=cbr:force-cfr=1", source)
	if err != nil {
		return err
	}
	if err := harness.finish("fixture", fixture, 20*time.Second); err != nil {
		return err
	}
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
	harness.results["fault_mode"], harness.results["input_bitrate"] = mode, bitrate
	harness.results["fault_scope"] = "public SDK through Go UploadPart fault proxy; backend identity is recorded separately; reset is an actual TCP reset, quota is an injected 507 XML response, not vendor quota or physical disk exhaustion"
	if err := harness.start(); err != nil {
		return err
	}
	defer harness.cleanup()
	publishers := make(map[string]*process)
	publishers[faultApp], err = harness.launch("publish-"+faultApp, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-re", "-stream_loop", "-1", "-i", source, "-t", "60", "-c", "copy", "-f", "flv", fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, faultApp))
	if err != nil {
		return err
	}
	publishers[healthyApp], err = harness.publish("publish-"+healthyApp, healthyApp, "bframes", 60)
	if err != nil {
		return err
	}
	players, watch, err := harness.faultPlayers()
	if watch != nil {
		defer func() { result = errors.Join(result, watch.finish(harness)) }()
	}
	if err != nil {
		return err
	}
	fault.enabled.Store(true)
	harness.results["fault_started_seconds"] = time.Since(harness.started).Seconds()
	failed, err := harness.waitRecording("upload failure visible", faultApp, 35*time.Second, func(state componentStatus) bool {
		return state.State == "failed" && state.Failures > 0 && state.LastFailureAt > 0
	})
	if err != nil {
		return err
	}
	harness.results["failed_recording_status"] = failed
	peer, err := harness.recordingStatus(healthyApp)
	if err != nil || peer.State != "active" || peer.Failures != 0 {
		return fmt.Errorf("healthy recorder was affected: %+v: %v", peer, err)
	}
	harness.results["healthy_peer_during_fault"] = peer
	harness.results["failure_observed_seconds"] = time.Since(harness.started).Seconds()
	harness.results["cancelled_requests_at_failure"] = fault.cancelled.Load()
	if mode == "queue" && fault.cancelled.Load() != 0 {
		return errors.New("queue failure was not observed before the blocked upload timed out")
	}
	*recorder = recorderConfig{Enable: true, Driver: "fs", SaveAs: faultApp, SaveMode: "immediate", Segment: 4, SaveTo: filepath.Join(harness.evidence, "recovered"), Event: callbacks.recordingEvent("recovered")}
	if err := harness.commit("recover failed upload to filesystem", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
		return err
	}
	recovered, err := harness.recordingStatus(faultApp)
	if err != nil || recovered.Failures < failed.Failures {
		return fmt.Errorf("failure history lost: %v", err)
	}
	harness.results["recovered_recording_status"] = recovered
	if err := harness.finishFaultMedia(players, publishers, func() error { return callbacks.wait(harness, faultApp, &fault.paths) }); err != nil {
		return err
	}
	if err := harness.recordings("recordings", 10, true); err != nil {
		return err
	}
	if err := harness.recordings("recovered", 4, true); err != nil {
		return err
	}
	if err := callbacks.verify(harness, faultApp, &fault.paths); err != nil {
		return err
	}
	pending, keys, err := backend.inventory(harness)
	if err != nil {
		return err
	}
	if pending != 0 || len(keys) != 0 {
		return fmt.Errorf("failed uncheckpointed upload left state: uploads=%d objects=%v", pending, keys)
	}
	harness.results["pending_uploads"] = 0
	harness.results["injected_requests"], harness.results["cancelled_requests"], harness.results["reset_connections"] = fault.requests.Load(), fault.cancelled.Load(), fault.resets.Load()
	if fault.requests.Load() == 0 || ((mode == "queue" || mode == "timeout") && fault.cancelled.Load() == 0) || (mode == "reset" && fault.resets.Load() == 0) {
		return errors.New("upload fault was not exercised")
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	expected, err := faultErrorsForApp(body, faultApp, func(message string) bool { return uploadErrorMatchesProvider(backend.provider, mode, message) })
	harness.results["expected_server_errors"] = expected
	if err != nil {
		return err
	}
	return harness.checkLogs("fixture.log", "publish-*.log", "play-*.log")
}
