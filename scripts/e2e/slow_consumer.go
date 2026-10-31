package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/rtmp/core"
)

type stalledSubscriber struct {
	connection *net.TCPConn
	reader     io.Reader
	protocol   string
	app        string
	local      string
}

func (harness *harness) stalledSubscriber(app, protocol string) (*stalledSubscriber, error) {
	location, err := url.Parse(harness.origin)
	if err != nil {
		return nil, err
	}
	address := location.Host
	if protocol == "rtmp" {
		address = fmt.Sprintf("127.0.0.1:%d", harness.rtmpPort)
	}
	dialer := net.Dialer{Timeout: 3 * time.Second}
	transport, err := dialer.DialContext(harness.ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	connection := transport.(*net.TCPConn)
	subscriber := &stalledSubscriber{connection: connection, reader: connection, protocol: protocol, app: app, local: connection.LocalAddr().String()}
	ready := false
	timer := time.AfterFunc(8*time.Second, func() { _ = connection.Close() })
	defer func() {
		timer.Stop()
		if !ready {
			_ = connection.Close()
		}
	}()
	if err := connection.SetReadBuffer(1024); err != nil {
		return nil, err
	}
	if protocol == "rtmp" {
		client := core.NewConnClient(app, "sample", core.NewConn(connection, 1024))
		if err := client.Start(core.Play); err != nil {
			return nil, err
		}
		var frame packet.Packet
		if err := core.NewReader(client).Read(&frame); err != nil {
			return nil, err
		}
		if len(frame.Data) == 0 {
			return nil, errors.New("slow RTMP viewer received no initial media")
		}
	} else {
		request, err := http.NewRequest(http.MethodGet, harness.origin+"/hfs/"+app+"/sample.flv", nil)
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
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("slow HTTP viewer status %d", response.StatusCode)
		}
		header := make([]byte, 9)
		if _, err := io.ReadFull(response.Body, header); err != nil {
			return nil, err
		}
		if string(header[:3]) != "FLV" {
			return nil, errors.New("slow HTTP viewer did not receive FLV")
		}
		subscriber.reader = response.Body
	}
	ready = true
	return subscriber, nil
}

func parseGoroutineMetric(body string) (int, error) {
	count := 0
	found := false
	for _, line := range strings.Split(body, "\n") {
		if value, exists := strings.CutPrefix(line, "go_goroutines "); exists {
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed <= 0 || found {
				return 0, errors.New("invalid or duplicate goroutine metric")
			}
			count, found = parsed, true
		}
	}
	if !found {
		return 0, errors.New("goroutine metric missing")
	}
	return count, nil
}

func (harness *harness) goroutines() (int, error) {
	status, body, err := harness.fetch("/metrics")
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("metrics status %d", status)
	}
	return parseGoroutineMetric(string(body))
}

func validateSlowConsumerLog(body string) (map[string]int, error) {
	counts := map[string]int{"timeout": 0, "flv": 0, "ts": 0}
	timeout := regexp.MustCompile(`^rtmp IO timeout\. elapse=[0-9]+s\. limit=10s$`)
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
		switch {
		case timeout.MatchString(event.Message):
			counts["timeout"]++
		case event.Message == "[flv/sample]Writer is timeout":
			counts["flv"]++
		case event.Message == "[ts/sample]Writer is timeout":
			counts["ts"]++
		default:
			return nil, fmt.Errorf("unexpected slow-consumer server error: %s", event.Message)
		}
	}
	if counts["timeout"] != 4 || counts["flv"] != 2 || counts["ts"] != 2 {
		return nil, fmt.Errorf("slow viewer eviction evidence incomplete: %v", counts)
	}
	return counts, nil
}

func (harness *harness) pressureFixture() (string, error) {
	return harness.pressureFixtureWithBitrate("4M")
}

func (harness *harness) pressureFixtureWithBitrate(bitrate string) (string, error) {
	fixture := filepath.Join(harness.evidence, "pressure.flv")
	generator, err := harness.launch("generate-pressure", "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror",
		"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25", "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=44100", "-t", "6",
		"-c:v", "libx264", "-preset", "veryfast", "-threads:v", "1", "-pix_fmt", "yuv420p", "-bf", "2", "-g", "50", "-keyint_min", "50", "-sc_threshold", "0",
		"-b:v", bitrate, "-minrate", bitrate, "-maxrate", bitrate, "-bufsize", bitrate, "-x264-params", "nal-hrd=cbr:force-cfr=1", "-c:a", "aac", "-b:a", "64k", "-f", "flv", fixture)
	if err != nil {
		return "", err
	}
	if err := harness.finish("generate-pressure", generator, 30*time.Second); err != nil {
		return "", err
	}
	return fixture, nil
}

func (harness *harness) slowConsumers() (failure error) {
	fixture, err := harness.pressureFixture()
	if err != nil {
		return err
	}
	harness.recordingApplications("immediate", 4, 6)
	harness.settings.RTMP.EdgeCheck = 1
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
	baseline, err := harness.subscribers()
	if err != nil {
		return err
	}
	before, err := harness.goroutines()
	if err != nil {
		return err
	}
	var slow []*stalledSubscriber
	defer func() {
		for _, viewer := range slow {
			_ = viewer.connection.Close()
		}
	}()
	for _, app := range []string{"flv", "ts"} {
		for _, protocol := range []string{"rtmp", "http-flv"} {
			viewer, err := harness.stalledSubscriber(app, protocol)
			if err != nil {
				return err
			}
			slow = append(slow, viewer)
		}
	}
	if err := harness.until("four stalled viewers registered", 3*time.Second, func() (bool, error) {
		current, err := harness.subscribers()
		return current["flv"].Count == baseline["flv"].Count+2 && current["ts"].Count == baseline["ts"].Count+2, err
	}); err != nil {
		return err
	}
	peak, err := harness.goroutines()
	if err != nil {
		return err
	}
	if peak < before+8 {
		return fmt.Errorf("stalled network handlers were not observed: before=%d peak=%d", before, peak)
	}
	retiredAt := time.Now()
	if err := harness.until("stalled viewers evicted while healthy viewers remain", 30*time.Second, func() (bool, error) {
		current, err := harness.subscribers()
		return current["flv"].Count == baseline["flv"].Count && current["ts"].Count == baseline["ts"].Count, err
	}); err != nil {
		return err
	}
	after := 0
	if err := harness.until("goroutines retire without resuming slow reads", 5*time.Second, func() (bool, error) {
		var err error
		after, err = harness.goroutines()
		return after <= before+2, err
	}); err != nil {
		return err
	}
	harness.results["slow_retirement_before_read_resumed"] = map[string]any{"clients": len(slow), "goroutines_before": before, "goroutines_peak": peak, "goroutines_retired": after, "maximum_residual_goroutines": 2, "eviction_seconds": time.Since(retiredAt).Seconds()}
	var closures []map[string]any
	for _, viewer := range slow {
		if err := viewer.connection.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			return err
		}
		buffered, err := io.Copy(io.Discard, io.LimitReader(viewer.reader, 16<<20))
		closed := err == nil || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.ErrUnexpectedEOF)
		if !closed || buffered >= 16<<20 {
			return fmt.Errorf("stalled %s/%s still connected after eviction: bytes=%d error=%v", viewer.app, viewer.protocol, buffered, err)
		}
		closures = append(closures, map[string]any{"app": viewer.app, "protocol": viewer.protocol, "local": viewer.local, "buffered_bytes_drained_after_retirement": buffered, "peer_closed": true, "terminal_error": fmt.Sprint(err)})
	}
	harness.results["stalled_socket_closures"] = closures
	var disconnected []map[string]string
	for _, app := range []string{"flv", "ts"} {
		for _, protocol := range []string{"rtmp", "http-flv"} {
			viewer, err := harness.stalledSubscriber(app, protocol)
			if err != nil {
				return err
			}
			if err := viewer.connection.Close(); err != nil {
				return err
			}
			disconnected = append(disconnected, map[string]string{"app": app, "protocol": protocol})
		}
	}
	if err := harness.until("disconnected viewers retire", 3*time.Second, func() (bool, error) {
		current, err := harness.subscribers()
		return current["flv"].Count == baseline["flv"].Count && current["ts"].Count == baseline["ts"].Count, err
	}); err != nil {
		return err
	}
	harness.results["explicit_disconnects"] = disconnected
	if err := harness.finishPlayers(players, 1000); err != nil {
		return err
	}
	for app, child := range publishers {
		if err := harness.finish("publish-"+app, child, 75*time.Second); err != nil {
			return err
		}
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	if err := harness.recordings("recordings", 1, true); err != nil {
		return err
	}
	if err := harness.checkLogs("play-*.log", "publish-*.log", "generate-*.log"); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	counts, err := validateSlowConsumerLog(string(body))
	harness.results["expected_slow_consumer_errors"] = counts
	return err
}
