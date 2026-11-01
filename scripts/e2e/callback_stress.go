package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/rtmp/core"
)

func (harness *harness) pressurePublisher(instance string) (*core.ConnClient, error) {
	return harness.callbackPublisher("pressure", instance)
}

func (harness *harness) callbackPublisher(app, instance string) (*core.ConnClient, error) {
	dialer := net.Dialer{Timeout: time.Second}
	transport, err := dialer.DialContext(harness.ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", harness.rtmpPort))
	if err != nil {
		return nil, err
	}
	if err := transport.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		_ = transport.Close()
		return nil, err
	}
	client := core.NewConnClient(app, instance, core.NewConn(transport, core.ConnBufSize))
	if err := client.StartPublish(); err != nil {
		_ = client.Close()
		return nil, err
	}
	if err := transport.SetDeadline(time.Time{}); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func (harness *harness) pressureCount(instance string) (int, error) {
	status, body, err := harness.fetch("/admin/stat/pub/info?key=" + url.QueryEscape("pressure/"+instance))
	if err != nil {
		return 0, err
	}
	var envelope struct {
		Code int `json:"code"`
		Data struct {
			Count int `json:"count"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return 0, err
	}
	if status != http.StatusOK || envelope.Code != 0 {
		return 0, errors.New("pressure publication query failed")
	}
	return envelope.Data.Count, nil
}

func (harness *harness) activeCallbackFrames() (map[string]int, error) {
	frames := map[string]int{}
	for _, app := range []string{"flv", "ts"} {
		name := app + "-rtmp"
		body, err := os.ReadFile(filepath.Join(harness.evidence, "progress-"+name+".txt"))
		if err != nil {
			return nil, err
		}
		frames[name], err = completeProgressFrames(string(body))
		if err != nil || frames[name] == 0 {
			return nil, fmt.Errorf("pressure baseline decoder %s not ready: %v", name, err)
		}
	}
	return frames, nil
}

func validateCallbackStressErrors(body []byte) ([]string, error) {
	expected := map[string]int{"[pressure/overflow]err=callback concurrency capacity exceeded,event=OnPublish": 1}
	for index := range 2 {
		expected[fmt.Sprintf("[pressure/shutdown-%d]err=callback request failed,event=OnPublish", index)] = 1
	}
	for _, app := range []string{"flv", "ts"} {
		for _, event := range []string{"OnPublishDone", "OnRecordDone"} {
			expected[fmt.Sprintf("[%s/sample]err=callback request failed,event=%s", app, event)] = 1
		}
	}
	warnings := []string{}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var event struct{ Level, Msg string }
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return warnings, err
		}
		if event.Level == "WARN" && event.Msg == "Not continuous timestamp" {
			warnings = append(warnings, event.Msg)
			continue
		}
		if event.Level != "ERROR" && event.Level != "WARN" {
			continue
		}
		if expected[event.Msg] != 1 {
			return warnings, fmt.Errorf("unexpected or duplicate callback pressure error: %s", event.Msg)
		}
		expected[event.Msg] = 0
	}
	for message, remaining := range expected {
		if remaining != 0 {
			return warnings, fmt.Errorf("missing callback pressure failure: %s", message)
		}
	}
	return warnings, nil
}

func validateCallbackStressObservations(observations []callbackObservation, cancelled []string) error {
	counts := map[string]int{}
	interrupted := map[string]int{}
	for _, observation := range observations {
		payload := observation.Payload
		key, keyValid := payload["key"].(string)
		event, eventValid := payload["event"].(string)
		occur, timeValid := payload["occur"].(float64)
		if !keyValid || !eventValid || !timeValid || occur <= 0 || occur > float64(observation.Received) {
			return errors.New("invalid pressure callback identity or time")
		}
		identity := key + "/" + event
		counts[identity]++
		if counts[identity] != 1 {
			return fmt.Errorf("duplicate pressure callback %s", identity)
		}
		if observation.Path == "/during-stop" {
			if (key != "flv/sample" && key != "ts/sample") || (event != "OnPublishDone" && event != "OnRecordDone") || observation.Status != 0 {
				return errors.New("unexpected terminal callback during shutdown")
			}
			if event == "OnRecordDone" {
				location, valid := payload["location"].(string)
				if len(payload) != 6 || !valid || !strings.HasPrefix(location, "/"+key+"/") || payload["filename"] != filepath.Base(location) {
					return errors.New("invalid recording completion during shutdown")
				}
			} else if len(payload) != 3 {
				return errors.New("invalid publication completion during shutdown")
			}
			interrupted[identity] = 1
			continue
		}
		if len(payload) != 3 || event != "OnPublish" {
			return errors.New("unexpected pressure event payload")
		}
		if key == "flv/sample" || key == "ts/sample" {
			if observation.Path != "/normal" || observation.Status != 200 {
				return errors.New("normal publication callback not acknowledged")
			}
			continue
		}
		known := key == "pressure/recovered"
		for index := range 32 {
			known = known || key == fmt.Sprintf("pressure/burst-%02d", index)
		}
		shutdown := key == "pressure/shutdown-0" || key == "pressure/shutdown-1"
		if observation.Path != "/hold" || (!known && !shutdown) || (known && observation.Status != 200) || (shutdown && observation.Status != 0) {
			return errors.New("unexpected pressure publication callback outcome")
		}
		if shutdown {
			interrupted[identity] = 1
		}
	}
	for index := range 32 {
		if counts[fmt.Sprintf("pressure/burst-%02d/OnPublish", index)] != 1 {
			return errors.New("missing acknowledged pressure callback")
		}
	}
	for _, key := range []string{"flv/sample", "ts/sample", "pressure/recovered", "pressure/shutdown-0", "pressure/shutdown-1"} {
		if counts[key+"/OnPublish"] != 1 {
			return fmt.Errorf("missing callback for %s", key)
		}
	}
	for _, identity := range cancelled {
		if interrupted[identity] != 1 {
			return fmt.Errorf("unexpected or duplicate receiver cancellation: %s", identity)
		}
		delete(interrupted, identity)
	}
	if len(interrupted) != 0 {
		return errors.New("received callback did not observe cancellation")
	}
	return nil
}

func (harness *harness) callbackStress() (failure error) {
	receiver := &combinationReceiver{release: make(chan struct{})}
	endpoint := httptest.NewServer(http.HandlerFunc(receiver.serve))
	defer endpoint.Close()
	defer receiver.releaseBurst()
	defer harness.captureCombinations(receiver)
	harness.recordingApplications("immediate", 4, 4)
	for index := range harness.settings.Application {
		harness.settings.Application[index].Event = &config.AppEvent{OnPublish: endpoint.URL + "/normal", OnPublishDone: endpoint.URL + "/during-stop"}
	}
	harness.settings.Application = append(harness.settings.Application, appConfig{Enable: true, AppName: "pressure", Event: &config.AppEvent{OnPublish: endpoint.URL + "/hold"}})
	if err := harness.start(); err != nil {
		return err
	}
	publishers := map[string]*process{}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		child, err := harness.publish("publish-"+app, app, "bframes", 90)
		if err != nil {
			return err
		}
		publishers[app] = child
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
		group, err := harness.players(app, app+"-", 36)
		if err != nil {
			return err
		}
		maps.Copy(players, group)
	}
	watch := harness.watchPlayback(players)
	defer func() { failure = errors.Join(failure, watch.finish(harness)) }()
	if err := watch.waitReady(harness.ctx); err != nil {
		return fmt.Errorf("callback pressure warmup: %w", err)
	}
	clients := make([]*core.ConnClient, 32)
	defer func() {
		for _, client := range clients {
			if client != nil {
				_ = client.Close()
			}
		}
	}()
	var workers sync.WaitGroup
	results := make(chan error, 32)
	burstStarted := time.Now()
	for index := range 32 {
		workers.Go(func() {
			client, err := harness.pressurePublisher(fmt.Sprintf("burst-%02d", index))
			clients[index] = client
			results <- err
		})
	}
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			return err
		}
	}
	if err := harness.until("32 callback requests actually held by independent receiver", time.Second, func() (bool, error) {
		_, state := receiver.snapshot()
		return state["active_burst"] == 32 && state["premature"] == 0, nil
	}); err != nil {
		return err
	}
	if time.Since(burstStarted) > 1200*time.Millisecond {
		return errors.New("callback pressure wave too slow for a valid bounded saturation window")
	}
	before, err := harness.activeCallbackFrames()
	if err != nil {
		return err
	}
	overflowStarted := time.Now()
	overflow, err := harness.pressurePublisher("overflow")
	if err != nil {
		return err
	}
	defer func() { _ = overflow.Close() }()
	overflowElapsed := time.Since(overflowStarted)
	if overflowElapsed > 500*time.Millisecond {
		return errors.New("full callback admission blocked RTMP publisher setup")
	}
	if err := harness.until("overflow event explicitly rejected", 300*time.Millisecond, func() (bool, error) {
		body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
		return bytesContainCallbackOverflow(body), err
	}); err != nil {
		return err
	}
	if err := harness.sleep(1100 * time.Millisecond); err != nil {
		return err
	}
	after, err := harness.activeCallbackFrames()
	if err != nil {
		return err
	}
	for name, previous := range before {
		if after[name] <= previous {
			return fmt.Errorf("decoder %s stopped while all callback slots were full", name)
		}
	}
	_, saturated := receiver.snapshot()
	if saturated["active_burst"] != 32 || saturated["premature"] != 0 {
		return errors.New("saturation ended before live progress was verified")
	}
	harness.results["callback_saturation"] = map[string]any{"capacity_observed": 32, "overflow_publication_setup_seconds": overflowElapsed.Seconds(), "frames_before": before, "frames_after": after, "hold_seconds": time.Since(burstStarted).Seconds()}
	receiver.releaseBurst()
	for _, client := range clients {
		_ = client.Close()
	}
	_ = overflow.Close()
	if err := harness.until("all pressure publishers retired", 2*time.Second, func() (bool, error) {
		for index := range 33 {
			instance := fmt.Sprintf("burst-%02d", index)
			if index == 32 {
				instance = "overflow"
			}
			count, err := harness.pressureCount(instance)
			if err != nil || count != 0 {
				return false, err
			}
		}
		_, state := receiver.snapshot()
		return state["active_burst"] == 0 && state["premature"] == 0, nil
	}); err != nil {
		return err
	}
	recovered, err := harness.pressurePublisher("recovered")
	if err != nil {
		return err
	}
	defer func() { _ = recovered.Close() }()
	if err := harness.until("callback admission resumes after pressure is released", time.Second, func() (bool, error) {
		observations, _ := receiver.snapshot()
		for _, observation := range observations {
			if observation.Payload["key"] == "pressure/recovered" && observation.Status == 200 {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return err
	}
	_ = recovered.Close()
	if err := harness.finishPlayers(players, 875); err != nil {
		return err
	}
	if err := watch.finish(harness); err != nil {
		return err
	}
	tailPlayers := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		for protocol, address := range map[string]string{"rtmp": fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app), "http-flv": harness.origin + "/hfs/" + app + "/sample.flv"} {
			name := app + "-" + protocol
			child, err := harness.launch("tail-"+name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000", "-i", address, "-t", "120", "-progress", filepath.Join(harness.evidence, "tail-progress-"+name+".txt"), "-f", "null", "-")
			if err != nil {
				return err
			}
			tailPlayers[name] = child
		}
	}
	activeFrames := map[string]int{}
	if err := harness.until("four active tail players before callback shutdown", 12*time.Second, func() (bool, error) {
		for name, child := range tailPlayers {
			if !child.alive() {
				return false, fmt.Errorf("tail player %s exited", name)
			}
			body, err := os.ReadFile(filepath.Join(harness.evidence, "tail-progress-"+name+".txt"))
			if err != nil {
				return false, err
			}
			frames, err := completeProgressFrames(string(body))
			if err != nil || frames < 25 {
				return false, err
			}
			activeFrames[name] = frames
		}
		return true, nil
	}); err != nil {
		return err
	}
	for index := range 2 {
		app := &harness.settings.Application[index]
		app.Recorder.SaveTo = filepath.Join(harness.evidence, "shutdown-recordings")
		app.Recorder.Event = &config.RecorderEvent{OnRecordDone: endpoint.URL + "/during-stop"}
	}
	if err := harness.commit("callbacks-created-by-active-recording-shutdown", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
		return err
	}
	for index := range 2 {
		client, err := harness.pressurePublisher(fmt.Sprintf("shutdown-%d", index))
		if err != nil {
			return err
		}
		defer func() { _ = client.Close() }()
	}
	if err := harness.until("two held callbacks while publishers and recordings remain active", time.Second, func() (bool, error) {
		_, state := receiver.snapshot()
		return state["shutdown_holds"] == 2 && state["premature"] == 0, nil
	}); err != nil {
		return err
	}
	if err := harness.sleep(300 * time.Millisecond); err != nil {
		return err
	}
	for app, publisher := range publishers {
		publication, err := harness.relayStat(app, "pub")
		if !publisher.alive() || err != nil || publication.Count != 1 {
			return fmt.Errorf("publisher %s not active at combined shutdown: %v", app, err)
		}
	}
	if err := harness.checkLogs("publish-*.log", "play-*.log", "tail-*.log"); err != nil {
		return err
	}
	receiver.mu.Lock()
	receiver.stopping = true
	receiver.mu.Unlock()
	shutdownStarted := time.Now()
	if err := harness.stopServer(); err != nil {
		return err
	}
	elapsed := time.Since(shutdownStarted).Seconds()
	if elapsed >= 3 {
		return fmt.Errorf("combined callback/media shutdown took %.3fs", elapsed)
	}
	disconnections := map[string]any{}
	for app, child := range publishers {
		code, err := child.wait(harness.ctx, 8*time.Second)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(harness.evidence, "publish-"+app+".log"))
		if err != nil {
			return err
		}
		if err := validateShutdownPublisher(code, string(body)); err != nil {
			return err
		}
		disconnections["publish-"+app] = map[string]any{"exit_code": code, "stderr": string(body), "active_before_sigterm": true}
	}
	for name, child := range tailPlayers {
		code, err := child.wait(harness.ctx, 8*time.Second)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(harness.evidence, "tail-"+name+".log"))
		if err != nil {
			return err
		}
		if err := validateInterruptedOrigin(code, string(body)); err != nil {
			return err
		}
		disconnections["tail-"+name] = map[string]any{"exit_code": code, "stderr": string(body), "decoded_frames_before_sigterm": activeFrames[name], "active_before_sigterm": true}
	}
	observations, final := receiver.snapshot()
	if final["invalid"] != "" || final["premature"] != 0 || final["active_burst"] != 0 || final["shutdown_holds"] != 0 {
		return fmt.Errorf("callback pressure ownership did not retire: %+v", final)
	}
	for index := range 2 {
		identity := fmt.Sprintf("pressure/shutdown-%d/OnPublish", index)
		found := 0
		for _, cancelled := range final["cancelled"].([]string) {
			if cancelled == identity {
				found++
			}
		}
		if found != 1 {
			return fmt.Errorf("held callback %s did not cancel at shutdown", identity)
		}
	}
	if err := validateCallbackStressObservations(observations, final["cancelled"].([]string)); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	warnings, err := validateCallbackStressErrors(body)
	if err != nil {
		return err
	}
	harness.results["preexisting_fragment_warnings"] = warnings
	harness.results["active_callback_shutdown"] = map[string]any{"seconds": elapsed, "publishers_active": 2, "players_active": 4, "pending_callbacks_before_sigterm": 2, "terminal_callbacks_cancelled": 4, "disconnections": disconnections}
	if err := harness.recordings("recordings", 20, true); err != nil {
		return err
	}
	if err := harness.recordings("shutdown-recordings", 2, true); err != nil {
		return err
	}
	if err := harness.checkLogs("play-*.log"); err != nil {
		return err
	}
	harness.results["callback_stress_verified"] = true
	return nil
}

func bytesContainCallbackOverflow(body []byte) bool {
	return strings.Contains(string(body), `"msg":"[pressure/overflow]err=callback concurrency capacity exceeded,event=OnPublish"`)
}
