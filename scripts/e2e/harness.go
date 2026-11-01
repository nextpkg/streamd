package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nextpkg/streamd/config"
)

type process struct {
	command *exec.Cmd
	done    chan struct{}
	err     error
}

func (child *process) alive() bool {
	select {
	case <-child.done:
		return false
	default:
		return true
	}
}

func (child *process) wait(ctx context.Context, timeout time.Duration) (int, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	case <-timer.C:
		return -1, fmt.Errorf("process timed out: %s", child.command.Args)
	case <-child.done:
		if child.err == nil {
			return 0, nil
		}
		var failure *exec.ExitError
		if errors.As(child.err, &failure) {
			return failure.ExitCode(), nil
		}
		return -1, child.err
	}
}

type harness struct {
	ctx        context.Context
	root       string
	evidence   string
	origin     string
	rtmpPort   int
	httpClient *http.Client
	server     *process
	children   []*process
	settings   serviceConfig
	results    map[string]any
	started    time.Time
	object     *objectFixture
	release    *releaseContainer
}

type listenerConfig struct {
	Addr        string `json:"addr"`
	ChunkSize   int    `json:"chunk_size,omitempty"`
	EdgeAlive   int    `json:"edge_alive,omitempty"`
	EdgeCheck   int    `json:"edge_check,omitempty"`
	DialTimeout int    `json:"dial_timeout,omitempty"`
	ReadTimeout int    `json:"read_timeout,omitempty"`
}

type serviceConfig struct {
	HTTP        listenerConfig  `json:"http"`
	RTMP        listenerConfig  `json:"rtmp"`
	LogLevel    string          `json:"log_level,omitempty"`
	Finder      *config.Finder  `json:"finder,omitempty"`
	Proxy       *config.Proxy   `json:"proxy,omitempty"`
	Console     *config.Console `json:"console,omitempty"`
	Metrics     *config.Metrics `json:"metrics,omitempty"`
	Application []appConfig     `json:"application"`
}

type appConfig struct {
	Enable     bool             `json:"enable"`
	AppName    string           `json:"app_name"`
	Passerby   bool             `json:"passerby,omitempty"`
	StaticPush []string         `json:"static_push,omitempty"`
	StaticPull []string         `json:"static_pull,omitempty"`
	Event      *config.AppEvent `json:"event,omitempty"`
	HLS        hlsConfig        `json:"hls"`
	Recorder   recorderConfig   `json:"recorder"`
}

type hlsConfig struct {
	Enable         bool   `json:"enable"`
	Fragment       int    `json:"fragment"`
	PlaylistLength int    `json:"playlist_length"`
	SecureKey      string `json:"secure_key,omitempty"`
	SecureTTL      int    `json:"secure_ttl,omitempty"`
}

type recorderConfig struct {
	Enable   bool                  `json:"enable"`
	SaveAs   string                `json:"save_as,omitempty"`
	SaveMode string                `json:"save_mode,omitempty"`
	Segment  int                   `json:"segment,omitempty"`
	Driver   string                `json:"driver,omitempty"`
	SaveTo   string                `json:"save_to,omitempty"`
	AWS      *objectConfig         `json:"aws,omitempty"`
	COS      *objectConfig         `json:"cos,omitempty"`
	Event    *config.RecorderEvent `json:"event,omitempty"`
}

type componentStatus struct {
	Component     string `json:"component"`
	Stream        string `json:"stream"`
	Revision      int    `json:"revision"`
	State         string `json:"state"`
	Failures      int    `json:"failures"`
	LastFailureAt int64  `json:"last_failure_at,omitempty"`
}

type configurationReport struct {
	Revision   int               `json:"_revision"`
	Components []componentStatus `json:"_components"`
}

func newHarness(ctx context.Context, root, prefix, marker string, race bool) (*harness, error) {
	for _, executable := range []string{"go", "ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(executable); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "artifacts"), 0700); err != nil {
		return nil, err
	}
	evidence, err := os.MkdirTemp(filepath.Join(root, "artifacts"), prefix)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(root, "artifacts", marker), []byte(evidence+"\n"), 0600); err != nil {
		return nil, err
	}
	httpPort, err := freePort()
	if err != nil {
		return nil, err
	}
	rtmpPort, err := freePort()
	for err == nil && httpPort == rtmpPort {
		rtmpPort, err = freePort()
	}
	if err != nil {
		return nil, err
	}
	harness := &harness{ctx: ctx, root: root, evidence: evidence, rtmpPort: rtmpPort,
		origin: fmt.Sprintf("http://127.0.0.1:%d", httpPort), httpClient: &http.Client{Timeout: 2 * time.Second},
		results: map[string]any{"race_build": race, "complete": false, "publish_seconds": 60}}
	harness.settings.HTTP.Addr = fmt.Sprintf("127.0.0.1:%d", httpPort)
	harness.settings.RTMP.Addr = fmt.Sprintf("127.0.0.1:%d", rtmpPort)
	buildContext, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	arguments := []string{"build"}
	cgo := "0"
	if race {
		arguments = append(arguments, "-race")
		cgo = "1"
	}
	arguments = append(arguments, "-o", filepath.Join(evidence, "streamd"), "./cmd/streamd")
	command := exec.CommandContext(buildContext, "go", arguments...)
	command.Dir = root
	command.Env = append(os.Environ(), "CGO_ENABLED="+cgo)
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("build service: %w: %s", err, output)
	}
	return harness, nil
}

func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func writeJSON(filename string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, append(encoded, '\n'), 0600)
}

func (harness *harness) launch(name string, arguments ...string) (*process, error) {
	output, err := os.OpenFile(filepath.Join(harness.evidence, name+".log"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	command := exec.Command(arguments[0], arguments[1:]...)
	command.Dir = harness.root
	command.Stdout, command.Stderr = output, output
	if err := command.Start(); err != nil {
		_ = output.Close()
		return nil, err
	}
	child := &process{command: command, done: make(chan struct{})}
	harness.children = append(harness.children, child)
	go func() {
		child.err = command.Wait()
		_ = output.Close()
		close(child.done)
	}()
	return child, nil
}

func (harness *harness) cleanup() {
	for _, child := range harness.children {
		if child.alive() {
			_ = child.command.Process.Signal(syscall.SIGTERM)
		}
	}
	for _, child := range harness.children {
		if _, err := child.wait(context.Background(), 10*time.Second); err != nil {
			_ = child.command.Process.Kill()
			<-child.done
		}
	}
	harness.httpClient.CloseIdleConnections()
	if !harness.started.IsZero() {
		harness.results["elapsed_seconds"] = time.Since(harness.started).Seconds()
	}
}

func (harness *harness) start() error {
	if harness.release != nil {
		return harness.release.start(harness)
	}
	filename := filepath.Join(harness.evidence, "config.json")
	if err := writeJSON(filename, harness.settings); err != nil {
		return err
	}
	server, err := harness.launch("server", filepath.Join(harness.evidence, "streamd"), "-config", filename)
	if err != nil {
		return err
	}
	harness.server = server
	if err := harness.until("server readiness", 15*time.Second, func() (bool, error) {
		status, _, err := harness.fetch("/readyz")
		return status == http.StatusOK, err
	}); err != nil {
		return err
	}
	harness.started = time.Now()
	return nil
}

func (harness *harness) fetch(location string) (int, []byte, error) {
	if !strings.HasPrefix(location, "http://") {
		location = harness.origin + location
	}
	request, err := http.NewRequestWithContext(harness.ctx, http.MethodGet, location, nil)
	if err != nil {
		return 0, nil, err
	}
	response, err := harness.httpClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, body, err
}

func (harness *harness) until(label string, timeout time.Duration, predicate func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	var lastError error
	for time.Now().Before(deadline) {
		if harness.server != nil && !harness.server.alive() {
			return fmt.Errorf("server exited while waiting for %s; inspect server.log", label)
		}
		ready, err := predicate()
		if err == nil && ready {
			return nil
		}
		lastError = err
		if err := harness.sleep(50 * time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("timeout waiting for %s: %v", label, lastError)
}

func (harness *harness) sleep(delay time.Duration) error {
	if delay <= 0 {
		return harness.ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-harness.ctx.Done():
		return harness.ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (harness *harness) waitAt(seconds int) error {
	return harness.sleep(time.Until(harness.started.Add(time.Duration(seconds) * time.Second)))
}

func (harness *harness) atomicSettings() error {
	encoded, err := json.MarshalIndent(harness.settings, "", "  ")
	if err != nil {
		return err
	}
	return harness.atomicConfiguration(append(encoded, '\n'))
}

func (harness *harness) atomicConfiguration(encoded []byte) error {
	filename := filepath.Join(harness.evidence, "candidate.json")
	mode := os.FileMode(0600)
	if harness.release != nil {
		mode = 0644
	}
	if err := os.WriteFile(filename, encoded, mode); err != nil {
		return err
	}
	if err := os.Rename(filename, filepath.Join(harness.evidence, "config.json")); err != nil {
		return err
	}
	if harness.release != nil {
		return harness.release.reload(harness)
	}
	return nil
}

func (harness *harness) configuration() (configurationReport, error) {
	var envelope struct {
		Data configurationReport `json:"data"`
	}
	status, body, err := harness.fetch("/admin/config")
	if err != nil {
		return envelope.Data, err
	}
	if status != http.StatusOK {
		return envelope.Data, fmt.Errorf("configuration HTTP status %d", status)
	}
	err = json.Unmarshal(body, &envelope)
	return envelope.Data, err
}

func (harness *harness) playlist(app string) (string, error) {
	status, body, err := harness.fetch("/hls/" + app + "/sample.m3u8")
	if err != nil {
		return "", err
	}
	if status != http.StatusOK || !strings.Contains(string(body), "#EXTINF") {
		return "", fmt.Errorf("playlist not ready: HTTP %d", status)
	}
	return string(body), nil
}

func (harness *harness) waitPlaylist(app string) (string, error) {
	var playlist string
	err := harness.until("playlist for "+app, 15*time.Second, func() (bool, error) {
		var err error
		playlist, err = harness.playlist(app)
		if err == nil && strings.Contains(playlist, "#EXT-X-ENDLIST") {
			err = fmt.Errorf("playlist for %s belongs to a stopped generation", app)
		}
		return err == nil, err
	})
	return playlist, err
}

func (harness *harness) publish(name, app, fixture string, seconds int) (*process, error) {
	return harness.publishFile(name, app, filepath.Join(harness.root, "testdata", "generated", fixture+".flv"), seconds)
}

func (harness *harness) publishFile(name, app, source string, seconds int) (*process, error) {
	return harness.launch(name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-re", "-stream_loop", "-1",
		"-i", source, "-t", strconv.Itoa(seconds), "-c", "copy", "-f", "flv",
		fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app))
}

func (harness *harness) players(app, prefix string, seconds int) (map[string]*process, error) {
	sources := map[string]string{"rtmp": fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app),
		"http-flv": harness.origin + "/hfs/" + app + "/sample.flv", "hls": harness.origin + "/hls/" + app + "/sample.m3u8"}
	players := make(map[string]*process)
	for protocol, source := range sources {
		name := prefix + protocol
		child, err := harness.launch("play-"+name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror",
			"-rw_timeout", "8000000", "-i", source, "-t", strconv.Itoa(seconds), "-progress", filepath.Join(harness.evidence, "progress-"+name+".txt"), "-f", "null", "-")
		if err != nil {
			return nil, err
		}
		players[name] = child
	}
	return players, nil
}

func (harness *harness) finish(name string, child *process, timeout time.Duration) error {
	code, err := child.wait(harness.ctx, timeout)
	harness.results[name] = code
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("%s exited with code %d", name, code)
	}
	return nil
}

func (harness *harness) finishPlayers(players map[string]*process, minimumFrames int) error {
	for name, child := range players {
		if err := harness.finish("play-"+name, child, 90*time.Second); err != nil {
			return err
		}
		progress, err := os.ReadFile(filepath.Join(harness.evidence, "progress-"+name+".txt"))
		if err != nil {
			return err
		}
		frames, err := validateDecodedFrames(string(progress), minimumFrames)
		harness.results["decoded_frames-"+name] = frames
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func (harness *harness) stopServer() error {
	if harness.release != nil {
		if err := harness.release.collect(harness); err != nil {
			return err
		}
	}
	if err := harness.server.command.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	return harness.finish("shutdown", harness.server, 15*time.Second)
}

func (harness *harness) checkLogs(patterns ...string) error {
	failures := make(map[string]string)
	for _, pattern := range patterns {
		filenames, err := filepath.Glob(filepath.Join(harness.evidence, pattern))
		if err != nil {
			return err
		}
		for _, filename := range filenames {
			data, err := os.ReadFile(filename)
			if err != nil {
				return err
			}
			if strings.TrimSpace(string(data)) != "" {
				failures[filepath.Base(filename)] = string(data)
			}
		}
	}
	harness.results["ffmpeg_errors"] = failures
	serverFiles, err := filepath.Glob(filepath.Join(harness.evidence, "server*.log"))
	if err != nil {
		return err
	}
	var serverLog strings.Builder
	for _, filename := range serverFiles {
		body, err := os.ReadFile(filename)
		if err != nil {
			return err
		}
		serverLog.Write(body)
	}
	race := strings.Contains(serverLog.String(), "WARNING: DATA RACE")
	harness.results["race_detected"] = race
	if race || len(failures) != 0 || strings.Contains(serverLog.String(), "panic:") {
		return errors.New("FFmpeg error, server panic, or data race; inspect evidence logs")
	}
	return nil
}

func segmentURL(origin, app, playlist string) (string, error) {
	base, err := url.Parse(origin + "/hls/" + app + "/sample.m3u8")
	if err != nil {
		return "", err
	}
	location := ""
	for _, line := range strings.Split(playlist, "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			location = line
		}
	}
	if location == "" {
		return "", errors.New("playlist contains no segment URL")
	}
	segment, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	return base.ResolveReference(segment).String(), nil
}
