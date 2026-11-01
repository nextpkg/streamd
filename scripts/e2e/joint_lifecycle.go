package main

import (
	"errors"
	"fmt"
	"maps"
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
	"syscall"
	"time"

	"github.com/nextpkg/streamd/config"
)

func (harness *harness) jointLifecycle() (result error) {
	backend, err := harness.startStorageFaultBackend()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, backend.close(harness)) }()
	if backend.provider != "aws" {
		return errors.New("joint lifecycle requires the actual S3-compatible backend")
	}
	registry, err := harness.startRedisContainer("joint")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, registry.close(harness)) }()
	inspection, err := newInspectionProxy(backend.endpoint, make(chan struct{}))
	if err != nil {
		return err
	}
	defer inspection.close()
	address, err := url.Parse(inspection.server.URL)
	if err != nil {
		return err
	}
	proxy := httputil.NewSingleHostReverseProxy(address)
	var enabled atomic.Bool
	var waiting atomic.Int64
	var held atomic.Int64
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	endpoint := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if enabled.Load() && request.Method == http.MethodPost && request.URL.Query().Get("uploadId") != "" && strings.Contains(request.URL.Path, "/flv/sample/") {
			held.Add(1)
			waiting.Add(1)
			defer waiting.Add(-1)
			select {
			case <-gate:
			case <-request.Context().Done():
				return
			}
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
	harness.settings.Finder = &config.Finder{Advice: "127.0.0.1", Expire: 30, Interval: 1, Separator: "/", Redis: &config.RedisConfig{Address: registry.address, Password: redisTestPassword, DB: 13}}
	if err := harness.start(); err != nil {
		return err
	}
	defer harness.cleanup()
	defer release()
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
		for protocol, location := range map[string]string{"rtmp": fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app), "flv": harness.origin + "/hfs/" + app + "/sample.flv"} {
			name := app + "-" + protocol
			players[name], err = harness.launch("tail-"+name, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-rw_timeout", "8000000", "-i", location, "-t", "60", "-progress", filepath.Join(harness.evidence, "progress-"+name+".txt"), "-f", "null", "-")
			if err != nil {
				return err
			}
		}
	}
	watch := harness.watchPlayback(players)
	defer func() { result = errors.Join(result, watch.finish(harness)) }()
	frames := map[string]int{}
	if err := harness.until("all four active media consumers decoded frames", 12*time.Second, func() (bool, error) {
		for name, player := range players {
			if !player.alive() {
				return false, errors.New("joint lifecycle player ended before shutdown")
			}
			body, err := os.ReadFile(filepath.Join(harness.evidence, "progress-"+name+".txt"))
			if errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			count, err := completeProgressFrames(string(body))
			if err != nil {
				return false, err
			}
			frames[name] = count
			if count < 25 {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err := harness.until("background inspection request active", 33*time.Second, func() (bool, error) {
		inspection.mu.Lock()
		defer inspection.mu.Unlock()
		return inspection.directories["flv/sample/"]+inspection.directories["ts/sample/"] > 0, nil
	}); err != nil {
		return err
	}
	enabled.Store(true)
	if err := harness.until("ordinary cloud finalizer active beside inspection", 5*time.Second, func() (bool, error) { return waiting.Load() > 0, nil }); err != nil {
		return err
	}
	if err := harness.sleep(750 * time.Millisecond); err != nil {
		return err
	}
	registration := map[string]any{}
	for _, app := range []string{"flv", "ts"} {
		key := "/streamd/" + app + "/sample"
		value, err := registry.client.Get(harness.ctx, key).Result()
		if err != nil || value != "127.0.0.1" {
			return fmt.Errorf("joint registry missing %s: %v", app, err)
		}
		ttl, err := registry.client.PTTL(harness.ctx, key).Result()
		if err != nil || ttl < 20*time.Second {
			return fmt.Errorf("joint registry TTL insufficient: %s %v", ttl, err)
		}
		registration[app] = map[string]any{"key": key, "ttl_milliseconds": ttl.Milliseconds(), "value": value}
		if !publishers[app].alive() {
			return errors.New("publisher not active at joint shutdown")
		}
	}
	if err := harness.waitRedisSubscriptions(registry, map[string]int{"default": 2}); err != nil {
		return err
	}
	before, err := registry.client.Info(harness.ctx, "commandstats").Result()
	if err != nil {
		return err
	}
	if err := harness.checkLogs("publish-*.log", "tail-*.log"); err != nil {
		return err
	}
	peers := []net.Conn{}
	defer func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()
	for index := 0; index < 4; index++ {
		peer, err := harness.pendingRTMP(index%2 == 0)
		if err != nil {
			return err
		}
		peers = append(peers, peer)
	}
	for name, player := range players {
		if !player.alive() {
			return fmt.Errorf("%s not active at SIGTERM", name)
		}
		body, err := os.ReadFile(filepath.Join(harness.evidence, "progress-"+name+".txt"))
		if err != nil {
			return err
		}
		count, err := completeProgressFrames(string(body))
		if err != nil || count <= frames[name] {
			return fmt.Errorf("%s stopped progressing before joint shutdown: %v", name, err)
		}
		frames[name] = count
	}
	if err := watch.finish(harness); err != nil {
		return err
	}
	started := time.Now()
	if err := harness.server.command.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	if err := harness.sleep(300 * time.Millisecond); err != nil {
		return err
	}
	if !harness.server.alive() || waiting.Load() == 0 {
		return errors.New("shutdown returned before admitted cloud finalizer completed")
	}
	harness.results["joint_waited_for_finalization"] = true
	release()
	if err := harness.finish("shutdown", harness.server, 5*time.Second); err != nil {
		return err
	}
	elapsed := time.Since(started).Seconds()
	if elapsed > 5 {
		return fmt.Errorf("joint shutdown took %.3f seconds", elapsed)
	}
	retired := false
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		inspection.mu.Lock()
		active, cancelled := 0, 0
		for prefix, count := range inspection.directories {
			if count > 0 && (prefix == "flv/sample/" || prefix == "ts/sample/") {
				active++
				if inspection.windows[prefix].Cancelled {
					cancelled++
				}
			}
		}
		retired = active > 0 && cancelled == active && waiting.Load() == 0
		inspection.mu.Unlock()
		if retired {
			break
		}
		if err := harness.sleep(20 * time.Millisecond); err != nil {
			return err
		}
	}
	if !retired {
		return errors.New("cloud proxy handlers or inspections survived shutdown")
	}
	if err := harness.waitRedisSubscriptions(registry, map[string]int{}); err != nil {
		return err
	}
	remaining, err := registry.client.Exists(harness.ctx, "/streamd/flv/sample", "/streamd/ts/sample").Result()
	if err != nil || remaining != 0 {
		return fmt.Errorf("registrations survived shutdown: %d %v", remaining, err)
	}
	after, err := registry.client.Info(harness.ctx, "commandstats").Result()
	if err != nil {
		return err
	}
	deletions := redisCommandCalls(after, "del") - redisCommandCalls(before, "del")
	if deletions != 2 {
		return fmt.Errorf("expected two explicit registration deletions, observed %d", deletions)
	}
	disconnections := map[string]any{}
	for app, publisher := range publishers {
		code, err := publisher.wait(harness.ctx, 8*time.Second)
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
		disconnections["publish-"+app] = map[string]any{"exit_code": code, "stderr": string(body)}
	}
	for name, player := range players {
		code, err := player.wait(harness.ctx, 8*time.Second)
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
		disconnections["tail-"+name] = map[string]any{"exit_code": code, "stderr": string(body), "frames_before_sigterm": frames[name]}
	}
	closedPeers := []map[string]any{}
	for _, peer := range peers {
		state, err := pendingPeerClosed(peer)
		if err != nil {
			return err
		}
		closedPeers = append(closedPeers, state)
	}
	inventory, err := harness.roundExpiryInventory(backend.reader, nil, nil, true)
	if err != nil {
		return err
	}
	if len(inventory) < 4 {
		return errors.New("joint shutdown has insufficient finalized recordings")
	}
	if err := harness.recordings("recordings", 4, true); err != nil {
		return err
	}
	if err := harness.cloudServerErrors(); err != nil {
		return err
	}
	inspection.mu.Lock()
	windows := maps.Clone(inspection.windows)
	inspection.mu.Unlock()
	harness.results["joint_shutdown"] = map[string]any{"seconds": elapsed, "registrations_before": registration, "explicit_redis_deletions": deletions, "subscriptions_after": 0, "registration_keys_after": 0, "blocked_completions": held.Load(), "inspection_windows": windows, "disconnections": disconnections, "half_connections": closedPeers, "completed_files": inventory}
	harness.results["joint_lifecycle_verified"] = true
	return nil
}
