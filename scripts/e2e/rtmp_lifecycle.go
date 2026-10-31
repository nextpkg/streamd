package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/nextpkg/streamd/rtmp/core"
)

func validateShutdownPublisher(code int, body string) error {
	if code != 224 && code != 202 {
		return fmt.Errorf("interrupted publisher exit=%d, expected observed EPIPE or ECONNRESET", code)
	}
	prefix := regexp.MustCompile(`^\[(vost#0:0/copy|aost#0:1/copy|out#0/flv) @ 0x[0-9a-fA-F]+\] (.+)$`)
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if code == 224 && len(lines) == 3 {
		sequence := []string{"Error submitting a packet to the muxer: End of file", "Error writing trailer: End of file", "Error closing file: Broken pipe"}
		for index, line := range lines {
			match := prefix.FindStringSubmatch(line)
			if len(match) != 3 || match[2] != sequence[index] || (index == 0 && !strings.HasSuffix(match[1], "/copy")) || (index != 0 && match[1] != "out#0/flv") {
				return fmt.Errorf("unexpected EOF-then-EPIPE publisher event: %q", line)
			}
		}
		return nil
	}
	message, reason := "Broken pipe", "-32 (Broken pipe)"
	if code == 202 {
		message, reason = "Connection reset by peer", "-54 (Connection reset by peer)"
	}
	expected := map[string]string{
		"Error submitting a packet to the muxer: " + message: "copy",
		"Error muxing a packet":                              "out#0/flv",
		"Task finished with error code: " + reason:           "out#0/flv",
		"Terminating thread with return code " + reason:      "out#0/flv",
		"Error writing trailer: " + message:                  "out#0/flv",
		"Error closing file: Broken pipe":                    "out#0/flv",
	}
	seen := map[string]bool{}
	repeated := false
	previous := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "Last message repeated 1 times" && previous == "Error submitting a packet to the muxer: "+message && !repeated {
			repeated = true
			continue
		}
		match := prefix.FindStringSubmatch(line)
		if len(match) != 3 {
			return fmt.Errorf("unexpected interrupted publisher log: %q", line)
		}
		kind, valid := expected[match[2]]
		if !valid || seen[match[2]] || (kind == "copy" && !strings.HasSuffix(match[1], "/copy")) || (kind != "copy" && match[1] != kind) {
			return fmt.Errorf("unexpected interrupted publisher event: %q", line)
		}
		seen[match[2]], previous = true, match[2]
	}
	if len(seen) != len(expected) {
		return errors.New("interrupted publisher lacks complete shutdown diagnostics")
	}
	return nil
}

func (harness *harness) pendingRTMP(handshake bool) (net.Conn, error) {
	dialer := net.Dialer{Timeout: 3 * time.Second}
	transport, err := dialer.DialContext(harness.ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", harness.rtmpPort))
	if err != nil {
		return nil, err
	}
	if handshake {
		if err := core.NewConn(transport, core.ConnBufSize).HandshakeClient(); err != nil {
			_ = transport.Close()
			return nil, err
		}
	}
	return transport, nil
}

func pendingPeerClosed(transport net.Conn) (map[string]any, error) {
	if err := transport.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		return nil, err
	}
	var body [1]byte
	count, err := transport.Read(body[:])
	if count != 0 || !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("idle RTMP peer did not receive EOF: bytes=%d error=%v", count, err)
	}
	return map[string]any{"local": transport.LocalAddr().String(), "peer_closed": true, "terminal_error": "EOF"}, nil
}

func (harness *harness) rtmpLifecycle() (failure error) {
	harness.recordingApplications("immediate", 4, 6)
	harness.settings.RTMP.ReadTimeout = 2
	harness.results["publish_seconds"] = 90
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", "bframes", 90)
	if err != nil {
		return err
	}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
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
		return fmt.Errorf("healthy playback warmup: %w", err)
	}
	baseline, err := harness.goroutines()
	if err != nil {
		return err
	}
	baselineSamples := []int{baseline}
	for sample := 0; sample < 4; sample++ {
		if err := harness.sleep(50 * time.Millisecond); err != nil {
			return err
		}
		observed, err := harness.goroutines()
		if err != nil {
			return err
		}
		baselineSamples = append(baselineSamples, observed)
		baseline = min(baseline, observed)
	}
	harness.results["negotiation_baseline_samples"] = baselineSamples
	var peers []net.Conn
	defer func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()
	for index := 0; index < 8; index++ {
		peer, err := harness.pendingRTMP(true)
		if err != nil {
			return err
		}
		peers = append(peers, peer)
	}
	peak, err := harness.goroutines()
	if err != nil || peak < baseline+8 {
		return fmt.Errorf("pending negotiation owners not observed: baseline=%d peak=%d error=%v", baseline, peak, err)
	}
	started := time.Now()
	retired := 0
	if err := harness.until("idle command handlers retire without client closure", 5*time.Second, func() (bool, error) {
		var err error
		retired, err = harness.goroutines()
		return retired <= baseline+2, err
	}); err != nil {
		return err
	}
	closures := []map[string]any{}
	for _, peer := range peers {
		result, err := pendingPeerClosed(peer)
		if err != nil {
			return err
		}
		closures = append(closures, result)
	}
	harness.results["pending_negotiation_retirement"] = map[string]any{"peers": closures, "goroutines_before": baseline, "goroutines_peak": peak, "goroutines_retired": retired, "seconds": time.Since(started).Seconds(), "budget_seconds": 2, "maximum_residual_goroutines": 2}
	expected := map[componentKey]string{}
	for index := range harness.settings.Application {
		app := &harness.settings.Application[index]
		app.HLS.PlaylistLength = 3
		expected[componentKey{"hls", app.AppName + "/sample"}] = "active"
		expected[componentKey{"recording", app.AppName + "/sample"}] = "active"
	}
	if err := harness.commit("reload-after-half-connection-retirement", expected); err != nil {
		return err
	}
	if err := harness.finishPlayers(players, 825); err != nil {
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
	if err := harness.until("four active players decode before shutdown", 12*time.Second, func() (bool, error) {
		for name, child := range tailPlayers {
			if !child.alive() {
				return false, fmt.Errorf("tail player %s exited before shutdown", name)
			}
			body, err := os.ReadFile(filepath.Join(harness.evidence, "tail-progress-"+name+".txt"))
			if errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			frames, err := completeProgressFrames(string(body))
			if err != nil {
				return false, err
			}
			activeFrames[name] = frames
			if frames < 25 {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err := harness.checkLogs("publish-*.log", "play-*.log", "tail-*.log"); err != nil {
		return err
	}
	harness.results["pre_shutdown_errors_empty"] = true
	for app, child := range publishers {
		if !child.alive() {
			return fmt.Errorf("publisher %s was not active at shutdown", app)
		}
		publication, err := harness.relayStat(app, "pub")
		if err != nil || publication.Count != 1 {
			return fmt.Errorf("publisher %s missing from service at shutdown: %+v %v", app, publication, err)
		}
	}
	for index := 0; index < 8; index++ {
		peer, err := harness.pendingRTMP(index%2 == 0)
		if err != nil {
			return err
		}
		peers = append(peers, peer)
	}
	shutdown := time.Now()
	if err := harness.stopServer(); err != nil {
		return err
	}
	elapsed := time.Since(shutdown).Seconds()
	if elapsed > 3 {
		return fmt.Errorf("active media shutdown took %.3fs", elapsed)
	}
	shutdownPeers := []map[string]any{}
	for index, peer := range peers[8:] {
		result, err := pendingPeerClosed(peer)
		if err != nil {
			return err
		}
		result["handshake_completed"] = index%2 == 0
		shutdownPeers = append(shutdownPeers, result)
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
	harness.results["active_media_shutdown"] = map[string]any{"seconds": elapsed, "exit_code": 0, "publishers_active": 2, "players_active": 4, "peers": shutdownPeers, "disconnections": disconnections}
	for _, app := range []string{"flv", "ts"} {
		if err := harness.recordings(filepath.Join("recordings", app), 8, true); err != nil {
			return err
		}
	}
	if err := harness.checkLogs("play-*.log"); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var event struct {
			Level   string `json:"level"`
			Message string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return err
		}
		if event.Level == "ERROR" {
			return fmt.Errorf("unexpected shutdown service error: %s", event.Message)
		}
	}
	harness.results["rtmp_lifecycle_verified"] = true
	return nil
}
