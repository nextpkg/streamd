package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type stalledSegment struct {
	connection *net.TCPConn
	response   *http.Response
	location   string
	local      string
	initial    int
}

func (harness *harness) largeSegment(app string) (string, []byte, error) {
	playlist, err := harness.playlist(app)
	if err != nil {
		return "", nil, err
	}
	for _, line := range strings.Split(playlist, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		location, err := segmentURL(harness.origin, app, line)
		if err != nil {
			return "", nil, err
		}
		status, body, err := harness.fetch(location)
		if err != nil {
			return "", nil, err
		}
		if status == http.StatusOK && len(body) >= 750000 {
			return location, body, nil
		}
	}
	return "", nil, errors.New("playlist has no pressure segment large enough to block a stalled socket")
}

func (harness *harness) stallSegment(location string) (*stalledSegment, error) {
	return harness.stallSegmentWithDialer(location, net.Dialer{Timeout: 3 * time.Second})
}

func (harness *harness) stallSegmentWithDialer(location string, dialer net.Dialer) (*stalledSegment, error) {
	parsed, err := url.Parse(location)
	if err != nil {
		return nil, err
	}
	transport, err := dialer.DialContext(harness.ctx, "tcp", parsed.Host)
	if err != nil {
		return nil, err
	}
	connection := transport.(*net.TCPConn)
	ready := false
	defer func() {
		if !ready {
			_ = connection.Close()
		}
	}()
	if err := connection.SetReadBuffer(1024); err != nil {
		return nil, err
	}
	if err := connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodGet, location, nil)
	if err != nil {
		return nil, err
	}
	if err := request.Write(connection); err != nil {
		return nil, err
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK || response.ContentLength < 750000 {
		return nil, fmt.Errorf("stalled segment status=%d bytes=%d", response.StatusCode, response.ContentLength)
	}
	prefix := make([]byte, 188)
	if _, err := io.ReadFull(response.Body, prefix); err != nil {
		return nil, err
	}
	if prefix[0] != 0x47 {
		return nil, errors.New("stalled segment is not a TS packet")
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	ready = true
	return &stalledSegment{connection: connection, response: response, location: location, local: connection.LocalAddr().String(), initial: len(prefix)}, nil
}

func (viewer *stalledSegment) closed() (map[string]any, error) {
	if err := viewer.connection.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return nil, err
	}
	count, err := io.Copy(io.Discard, viewer.response.Body)
	if !errors.Is(err, io.ErrUnexpectedEOF) || count+int64(viewer.initial) >= viewer.response.ContentLength {
		return nil, fmt.Errorf("slow HLS response not truncated and closed: received=%d declared=%d error=%v", count+int64(viewer.initial), viewer.response.ContentLength, err)
	}
	return map[string]any{"location": viewer.location, "local": viewer.local, "content_length": viewer.response.ContentLength, "received_after_retirement": count + int64(viewer.initial), "peer_closed": true, "terminal_error": err.Error()}, nil
}

func slowHLSErrors(body string, origin string, expected map[string]bool) (map[string]int, error) {
	parsed, err := url.Parse(origin)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		var event struct {
			Level   string `json:"level"`
			Message string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, err
		}
		if event.Level != "ERROR" {
			continue
		}
		prefix := "write tcp " + parsed.Host + "->"
		address, found := strings.CutPrefix(event.Message, prefix)
		address, timedOut := strings.CutSuffix(address, ": i/o timeout")
		if !found || !timedOut || !expected[address] {
			return nil, fmt.Errorf("unexpected slow-HLS server error: %s", event.Message)
		}
		counts[address]++
		if counts[address] != 1 {
			return nil, errors.New("duplicate error for stalled response")
		}
	}
	return counts, nil
}

func (harness *harness) slowHLS() (failure error) {
	fixture, err := harness.pressureFixtureWithBitrate("16M")
	if err != nil {
		return err
	}
	harness.results["pressure_video_bitrate"] = "16M"
	harness.recordingApplications("immediate", 4, 6)
	if err := harness.start(); err != nil {
		return err
	}
	publishers := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		child, err := harness.launch("publish-"+app, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-re", "-stream_loop", "-1", "-i", fixture, "-t", "60", "-c", "copy", "-f", "flv", fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app))
		if err != nil {
			return err
		}
		publishers[app] = child
	}
	players, err := harness.playersForApps(45)
	if err != nil {
		return err
	}
	watch := harness.watchPlayback(players)
	defer func() { failure = errors.Join(failure, watch.finish(harness)) }()
	select {
	case <-watch.ready:
	case <-watch.done:
		return fmt.Errorf("healthy playback warmup: %w", watch.err)
	case <-harness.ctx.Done():
		return harness.ctx.Err()
	}
	before, err := harness.goroutines()
	if err != nil {
		return err
	}
	var viewers []*stalledSegment
	defer func() {
		for _, viewer := range viewers {
			_ = viewer.connection.Close()
		}
	}()
	media := map[string][]byte{}
	for _, app := range []string{"flv", "ts"} {
		location, body, err := harness.largeSegment(app)
		if err != nil {
			return err
		}
		media[location] = body
		if err := os.MkdirAll(filepath.Join(harness.evidence, "segments"), 0700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(harness.evidence, "segments", app+".ts"), body, 0600); err != nil {
			return err
		}
		for repeat := 0; repeat < 2; repeat++ {
			viewer, err := harness.stallSegment(location)
			if err != nil {
				return err
			}
			viewers = append(viewers, viewer)
		}
	}
	peak, err := harness.goroutines()
	if err != nil {
		return err
	}
	if peak < before+8 {
		return fmt.Errorf("slow HLS response owners not observed: before=%d peak=%d", before, peak)
	}
	expected := map[componentKey]string{}
	for index := range harness.settings.Application {
		app := &harness.settings.Application[index]
		app.HLS.PlaylistLength = 3
		expected[componentKey{"hls", app.AppName + "/sample"}] = "active"
		expected[componentKey{"recording", app.AppName + "/sample"}] = "active"
	}
	if err := harness.commit("resize-playlist-with-stalled-responses", expected); err != nil {
		return err
	}
	started := time.Now()
	after := 0
	if err := harness.until("slow HLS handlers retire without resumed reads", 15*time.Second, func() (bool, error) {
		body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
		if err != nil {
			return false, err
		}
		expected := map[string]bool{}
		for _, viewer := range viewers {
			expected[viewer.local] = true
		}
		counts, err := slowHLSErrors(string(body), harness.origin, expected)
		if err != nil {
			return false, err
		}
		for peer := range expected {
			if counts[peer] != 1 {
				return false, nil
			}
		}
		after, err = harness.goroutines()
		return after <= before+2, err
	}); err != nil {
		return err
	}
	harness.results["hls_retirement_before_read_resumed"] = map[string]any{"clients": len(viewers), "goroutines_before": before, "goroutines_peak": peak, "goroutines_retired": after, "maximum_residual_goroutines": 2, "seconds_after_reload": time.Since(started).Seconds()}
	closures := []map[string]any{}
	for _, viewer := range viewers {
		result, err := viewer.closed()
		if err != nil {
			return err
		}
		closures = append(closures, result)
	}
	harness.results["expired_hls_responses"] = closures
	recovered := []map[string]any{}
	for location, expected := range media {
		status, body, err := harness.fetch(location)
		if err != nil {
			return err
		}
		if status != http.StatusOK || !bytes.Equal(body, expected) {
			return fmt.Errorf("slow response or resize changed retained segment: status=%d bytes=%d", status, len(body))
		}
		recovered = append(recovered, map[string]any{"location": location, "bytes": len(body), "sha256": fmt.Sprintf("%x", sha256.Sum256(body)), "identical_after_failures_and_reload": true})
	}
	harness.results["retained_hls_segments"] = recovered
	if err := harness.finishPlayers(players, 1000); err != nil {
		return err
	}
	for app, child := range publishers {
		if err := harness.finish("publish-"+app, child, 75*time.Second); err != nil {
			return err
		}
	}
	for _, app := range []string{"flv", "ts"} {
		location, _, err := harness.largeSegment(app)
		if err != nil {
			return err
		}
		for repeat := 0; repeat < 2; repeat++ {
			viewer, err := harness.stallSegment(location)
			if err != nil {
				return err
			}
			viewers = append(viewers, viewer)
		}
	}
	shutdown := time.Now()
	if err := harness.stopServer(); err != nil {
		return err
	}
	elapsed := time.Since(shutdown).Seconds()
	if elapsed > 3 {
		return fmt.Errorf("slow HLS responses delayed graceful shutdown %.3fs", elapsed)
	}
	shutdownClosures := []map[string]any{}
	for _, viewer := range viewers[4:] {
		result, err := viewer.closed()
		if err != nil {
			return err
		}
		shutdownClosures = append(shutdownClosures, result)
	}
	harness.results["graceful_shutdown_with_slow_hls"] = map[string]any{"seconds": elapsed, "responses": shutdownClosures, "publishers_already_completed": true, "exit_code": 0}
	for _, directory := range []string{"recordings", "segments"} {
		if err := harness.recordings(directory, 1, true); err != nil {
			return err
		}
	}
	if err := harness.checkLogs("play-*.log", "publish-*.log", "generate-*.log"); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	addresses := map[string]bool{}
	for _, viewer := range viewers {
		addresses[viewer.local] = true
	}
	counts, err := slowHLSErrors(string(body), harness.origin, addresses)
	if err != nil {
		return err
	}
	for _, viewer := range viewers[:4] {
		if counts[viewer.local] != 1 {
			return errors.New("missing deadline error for stalled HLS response")
		}
	}
	harness.results["expected_hls_deadline_errors"] = counts
	return nil
}
