package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/config"
)

func (harness *harness) adminData(ctx context.Context, path string) (map[string]any, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, harness.origin+path, nil)
	if err != nil {
		return nil, err
	}
	response, err := harness.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var envelope struct {
		Code    int
		Message string
		Data    map[string]any
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		return nil, err
	}
	if response.StatusCode != 200 || envelope.Code != 0 || envelope.Data == nil {
		return nil, fmt.Errorf("admin %s status=%d code=%d", path, response.StatusCode, envelope.Code)
	}
	return envelope.Data, nil
}

func validateAdminRows(data map[string]any, key string, count int) error {
	rows, _ := data["list"].([]any)
	if data["count"] != float64(count) || len(rows) != count || len(data) != 2 {
		return fmt.Errorf("%s count/list mismatch: %v", key, data)
	}
	for _, raw := range rows {
		row, valid := raw.(map[string]any)
		if !valid || len(row) != 7 || row["key"] != key {
			return errors.New("admin row schema/key mismatch")
		}
		for _, field := range []string{"pts", "duration", "videoReceived", "videoSpeed", "audioReceived", "audioSpeed"} {
			value, valid := row[field].(float64)
			if !valid || value < 0 {
				return fmt.Errorf("invalid admin counter %s", field)
			}
		}
		if row["videoReceived"].(float64) == 0 || row["audioReceived"].(float64) == 0 || row["pts"].(float64) == 0 {
			return errors.New("media counters have not advanced")
		}
	}
	return nil
}

func validateAdminStage(snapshot map[string]any, publishers, subscribers, online int, checkin bool) error {
	keys := snapshot["keys"].(map[string]any)
	list, _ := keys["list"].([]any)
	if keys["count"] != float64(len(list)) || len(keys) != 2 {
		return errors.New("admin key list is inconsistent")
	}
	names := []string{}
	for _, key := range list {
		text, valid := key.(string)
		if !valid {
			return errors.New("non-string stream key")
		}
		names = append(names, text)
	}
	sort.Strings(names)
	if publishers != 0 && strings.Join(names, ",") != "flv/sample,ts/sample" {
		return fmt.Errorf("unexpected stream keys %v", names)
	}
	for _, app := range []string{"flv", "ts"} {
		if err := validateAdminRows(snapshot[app+"-pub"].(map[string]any), app+"/sample", publishers); err != nil {
			return err
		}
		if err := validateAdminRows(snapshot[app+"-sub"].(map[string]any), app+"/sample", subscribers); err != nil {
			return err
		}
		active := snapshot[app+"-online"].(map[string]any)
		stamp, valid := active["checkin"].(float64)
		if len(active) != 2 || active["count"] != float64(online) || !valid || stamp < 0 || stamp > float64(time.Now().Unix()) || (checkin && stamp == 0) {
			return fmt.Errorf("invalid external viewer count/checkin: %v", active)
		}
	}
	return nil
}

func (harness *harness) adminStage(label string, publishers, subscribers, online int, checkin bool) error {
	return harness.until("admin stage "+label, 3*time.Second, func() (bool, error) {
		snapshot := map[string]any{}
		paths := map[string]string{"keys": "/admin/stat/keys"}
		for _, app := range []string{"flv", "ts"} {
			paths[app+"-pub"] = "/admin/stat/pub/info?key=" + app + "/sample"
			paths[app+"-sub"] = "/admin/stat/sub/info?key=" + app + "/sample"
			paths[app+"-online"] = "/admin/stat/sub/online?key=" + app + "/sample"
		}
		for name, path := range paths {
			data, err := harness.adminData(harness.ctx, path)
			if err != nil {
				return false, err
			}
			snapshot[name] = data
		}
		if err := validateAdminStage(snapshot, publishers, subscribers, online, checkin); err != nil {
			return false, err
		}
		stages, _ := harness.results["admin_stages"].(map[string]any)
		if stages == nil {
			stages = map[string]any{}
			harness.results["admin_stages"] = stages
		}
		stages[label] = snapshot
		return true, nil
	})
}

type stalledAdmin struct {
	connection *net.TCPConn
	response   *http.Response
	read       int
	started    time.Time
}

func (harness *harness) stallAdmin() (*stalledAdmin, error) {
	address, err := url.Parse(harness.origin)
	if err != nil {
		return nil, err
	}
	transport, err := net.DialTimeout("tcp", address.Host, 2*time.Second)
	if err != nil {
		return nil, err
	}
	connection := transport.(*net.TCPConn)
	ready := false
	defer func() {
		if !ready {
			connection.Close()
		}
	}()
	if err := connection.SetReadBuffer(1024); err != nil {
		return nil, err
	}
	if err := connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return nil, err
	}
	started := time.Now()
	if _, err := fmt.Fprintf(connection, "GET /admin/config HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", address.Host); err != nil {
		return nil, err
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("slow admin HTTP %d", response.StatusCode)
	}
	if _, err := io.ReadFull(response.Body, make([]byte, 1024)); err != nil {
		return nil, err
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	ready = true
	return &stalledAdmin{connection: connection, response: response, read: 1024, started: started}, nil
}

func (client *stalledAdmin) retired(expected int) (map[string]any, error) {
	if err := client.connection.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return nil, err
	}
	count, err := io.Copy(io.Discard, client.response.Body)
	if !errors.Is(err, io.ErrUnexpectedEOF) || int(count)+client.read >= expected {
		return nil, fmt.Errorf("management response not truncated: bytes=%d expected=%d error=%v", int(count)+client.read, expected, err)
	}
	return map[string]any{"received_bytes": int(count) + client.read, "expected_bytes": expected, "terminal_error": err.Error(), "peer_closed": true, "observed_seconds": time.Since(client.started).Seconds()}, nil
}

type adminPolls struct {
	cancel  context.CancelFunc
	done    chan struct{}
	counts  map[string]int
	maximum float64
	err     error
}

func (harness *harness) pollAdmin() *adminPolls {
	ctx, cancel := context.WithCancel(harness.ctx)
	group := &adminPolls{cancel: cancel, done: make(chan struct{}), counts: map[string]int{}}
	go func() {
		defer close(group.done)
		var workers sync.WaitGroup
		var guard sync.Mutex
		for _, path := range []string{"/admin/stat/keys", "/admin/stat/pub/info?key=flv/sample", "/admin/stat/sub/info?key=ts/sample", "/admin/stat/sub/online?key=flv/sample"} {
			workers.Add(1)
			go func(path string) {
				defer workers.Done()
				ticker := time.NewTicker(50 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						started := time.Now()
						_, err := harness.adminData(ctx, path)
						if ctx.Err() != nil {
							return
						}
						guard.Lock()
						group.maximum = max(group.maximum, time.Since(started).Seconds())
						if err != nil {
							group.err = err
						} else {
							group.counts[path]++
						}
						guard.Unlock()
						if err != nil {
							return
						}
					}
				}
			}(path)
		}
		workers.Wait()
	}()
	return group
}

func (group *adminPolls) finish() error {
	group.cancel()
	<-group.done
	if group.err != nil {
		return group.err
	}
	for _, count := range group.counts {
		if count < 100 {
			return errors.New("too few concurrent admin queries")
		}
	}
	if len(group.counts) != 4 || group.maximum > 2 {
		return errors.New("admin polling missing or stalled")
	}
	return nil
}

func (harness *harness) adminStatistics() error {
	var callbacksMu sync.Mutex
	callbacks := []callbackObservation{}
	endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(writer, "invalid", 400)
			return
		}
		callbacksMu.Lock()
		callbacks = append(callbacks, callbackObservation{Path: request.URL.Path, Received: time.Now().UnixMilli(), Payload: payload, Status: 503})
		callbacksMu.Unlock()
		writer.WriteHeader(503)
	}))
	defer endpoint.Close()
	defer func() {
		callbacksMu.Lock()
		defer callbacksMu.Unlock()
		harness.results["admin_callback_observations"] = callbacks
	}()
	harness.recordingApplications("immediate", 4, 3)
	for index := range harness.settings.Application {
		harness.settings.Application[index].Recorder.Event = &config.RecorderEvent{OnRecord: endpoint.URL + "/record"}
	}
	harness.settings.Application = append(harness.settings.Application, appConfig{Enable: false, AppName: "padding", StaticPull: []string{"rtmp://127.0.0.1/" + strings.Repeat("x", 4*1024*1024)}})
	if err := harness.start(); err != nil {
		return err
	}
	_, configuration, err := harness.fetch("/admin/config")
	if err != nil {
		return err
	}
	if len(configuration) < 4*1024*1024 {
		return errors.New("large management response fixture missing")
	}
	harness.results["admin_response_bytes"] = len(configuration)
	publishers := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		child, err := harness.publish("publish-initial-"+app, app, "regular", 70)
		if err != nil {
			return err
		}
		publishers[app] = child
	}
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
	}
	if err := harness.adminStage("before-viewers", 1, 1, 0, false); err != nil {
		return err
	}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		group, err := harness.players(app, app+"-", 42)
		if err != nil {
			return err
		}
		for name, child := range group {
			players[name] = child
		}
	}
	watch := harness.watchPlayback(players)
	defer watch.cancel()
	traceContext, stopTrace := context.WithCancel(harness.ctx)
	traceDone := make(chan struct{})
	playlistTrace := []map[string]any{}
	go func() {
		defer close(traceDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-traceContext.Done():
				return
			case <-ticker.C:
				for _, app := range []string{"flv", "ts"} {
					request, err := http.NewRequestWithContext(traceContext, http.MethodGet, harness.origin+"/hls/"+app+"/sample.m3u8", nil)
					if err != nil {
						return
					}
					response, err := harness.httpClient.Do(request)
					if err != nil {
						return
					}
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					if err != nil {
						return
					}
					playlistTrace = append(playlistTrace, map[string]any{"elapsed": time.Since(harness.started).Seconds(), "app": app, "status": response.StatusCode, "body": string(body)})
				}
			}
		}
	}()
	defer func() { stopTrace(); <-traceDone; harness.results["admin_playlist_trace"] = playlistTrace }()
	select {
	case <-watch.ready:
	case <-watch.done:
		return watch.err
	case <-harness.ctx.Done():
		return harness.ctx.Err()
	}
	if err := harness.adminStage("live-viewers", 1, 3, 2, true); err != nil {
		return err
	}
	polls := harness.pollAdmin()
	defer func() { polls.cancel(); <-polls.done }()
	clients := []*stalledAdmin{}
	defer func() {
		for _, client := range clients {
			client.connection.Close()
		}
	}()
	for range 2 {
		client, err := harness.stallAdmin()
		if err != nil {
			return err
		}
		clients = append(clients, client)
	}
	for index := 0; index < 2; index++ {
		harness.settings.Application[index].HLS.PlaylistLength = 4
	}
	if err := harness.commit("admin-queries-and-callback-failures-with-live-reload", nil); err != nil {
		return err
	}
	if err := harness.sleep(5500 * time.Millisecond); err != nil {
		return err
	}
	retirements := []map[string]any{}
	for _, client := range clients {
		result, err := client.retired(len(configuration))
		if err != nil {
			return err
		}
		retirements = append(retirements, result)
		client.connection.Close()
	}
	harness.results["admin_slow_response_retirements"] = retirements
	if err := harness.waitAt(20); err != nil {
		return err
	}
	before, err := harness.subscribers()
	if err != nil {
		return err
	}
	disconnections := map[string]any{}
	for _, app := range []string{"flv", "ts"} {
		previous := publishers[app]
		child, err := harness.publish("publish-final-"+app, app, "bframes", 40)
		if err != nil {
			return err
		}
		publishers[app] = child
		code, err := previous.wait(harness.ctx, 5*time.Second)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(harness.evidence, "publish-initial-"+app+".log"))
		if err != nil {
			return err
		}
		if err := validateShutdownPublisher(code, string(body)); err != nil {
			return err
		}
		disconnections[app] = map[string]any{"exit_code": code, "stderr": string(body)}
	}
	harness.results["admin_takeover_disconnections"] = disconnections
	if err := harness.sleep(2 * time.Second); err != nil {
		return err
	}
	after, err := harness.subscribers()
	if err != nil {
		return err
	}
	for app, snapshot := range before {
		if err := verifyContinuity(snapshot, after[app]); err != nil {
			return err
		}
	}
	harness.results["admin_takeover_continuity"] = map[string]any{"before": before, "after": after}
	if err := harness.adminStage("after-takeover", 1, 3, 2, true); err != nil {
		return err
	}
	if err := polls.finish(); err != nil {
		return err
	}
	harness.results["admin_concurrent_queries"] = map[string]any{"counts": polls.counts, "maximum_seconds": polls.maximum}
	if err := harness.finishPlayers(players, 1000); err != nil {
		return err
	}
	playbackFailure := watch.finish(harness)
	if playbackFailure != nil {
		harness.results["admin_playback_failure"] = playbackFailure.Error()
	}
	if err := harness.adminStage("after-viewers", 1, 1, 0, true); err != nil {
		return err
	}
	for app, child := range publishers {
		if err := harness.finish("publish-final-"+app, child, 30*time.Second); err != nil {
			return err
		}
	}
	if err := harness.adminStage("after-publications", 0, 0, 0, false); err != nil {
		return err
	}
	closing := []*stalledAdmin{}
	for range 2 {
		client, err := harness.stallAdmin()
		if err != nil {
			return err
		}
		clients = append(clients, client)
		closing = append(closing, client)
	}
	started := time.Now()
	if err := harness.stopServer(); err != nil {
		return err
	}
	if time.Since(started) > 3*time.Second {
		return errors.New("pending management responses delayed shutdown")
	}
	shutdown := map[string]any{"seconds": time.Since(started).Seconds(), "pending": len(closing)}
	retirements = nil
	for _, client := range closing {
		result, err := client.retired(len(configuration))
		if err != nil {
			return err
		}
		retirements = append(retirements, result)
	}
	shutdown["retirements"] = retirements
	harness.results["admin_query_shutdown"] = shutdown
	for _, app := range []string{"flv", "ts"} {
		if err := harness.recordings(filepath.Join("recordings", app), 8, false); err != nil {
			return err
		}
	}
	if err := harness.takeoverRecordingIdentity(); err != nil {
		return err
	}
	if err := harness.checkLogs("play-*.log", "publish-final-*.log"); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	observedFailures := 0
	warnings := []string{}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var event struct{ Level, Msg string }
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return err
		}
		if event.Level == "WARN" && event.Msg == "Not continuous timestamp" {
			warnings = append(warnings, event.Msg)
			continue
		}
		if event.Level != "ERROR" && event.Level != "WARN" {
			continue
		}
		if event.Msg != "[flv/sample]err=callback returned HTTP 503,event=OnRecord" && event.Msg != "[ts/sample]err=callback returned HTTP 503,event=OnRecord" {
			return fmt.Errorf("unexpected admin scenario diagnostic: %s", event.Msg)
		}
		observedFailures++
	}
	callbacksMu.Lock()
	count := len(callbacks)
	callbacksMu.Unlock()
	if count == 0 || count != observedFailures {
		return errors.New("callback rejection ledger mismatch")
	}
	harness.results["admin_callback_rejections"] = count
	harness.results["preexisting_fragment_warnings"] = warnings
	harness.results["admin_statistics_verified"] = true
	return playbackFailure
}
