package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nextpkg/streamd/rtmp/core"
)

func (harness *harness) rejectApplicationRTMP(app, instance, method string) error {
	transport, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", harness.rtmpPort), time.Second)
	if err != nil {
		return err
	}
	defer transport.Close()
	if err := transport.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	client := core.NewConnClient(app, instance, core.NewConn(transport, core.ConnBufSize))
	err = client.Start(method)
	for err == nil {
		var response core.ChunkStream
		err = client.Read(&response)
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		return fmt.Errorf("%s %s/%s did not close promptly: %w", method, app, instance, err)
	}
	return nil
}

func (harness *harness) unavailableApplication(label, app string, live bool) error {
	for _, method := range []string{core.Play, core.Publish} {
		if err := harness.rejectApplicationRTMP(app, "sample", method); err != nil {
			return err
		}
	}
	checks := map[string]int{
		"/hfs/" + app + "/absent.flv":  http.StatusOK,
		"/hls/" + app + "/absent.m3u8": http.StatusNotFound,
		"/hls/" + app + "/absent.ts":   http.StatusNotFound,
	}
	if !live {
		checks["/hfs/"+app+"/sample.flv"] = http.StatusOK
		checks["/hls/"+app+"/sample.m3u8"] = http.StatusNotFound
	}
	for path, expected := range checks {
		status, body, err := harness.fetch(path)
		if err != nil || status != expected || (expected == http.StatusOK && len(body) != 0) {
			return fmt.Errorf("%s %s: HTTP %d bytes=%d error=%v", label, path, status, len(body), err)
		}
	}
	keys, err := harness.adminData(harness.ctx, "/admin/stat/keys")
	if err != nil {
		return err
	}
	if keys["count"] != float64(2) {
		return fmt.Errorf("%s rejected requests changed stream count: %v", label, keys)
	}
	checksByStage, _ := harness.results["application_rejections"].(map[string]any)
	if checksByStage == nil {
		checksByStage = map[string]any{}
		harness.results["application_rejections"] = checksByStage
	}
	checksByStage[label+"/"+app] = map[string]any{"rtmp_publish_closed": true, "rtmp_play_closed": true, "http": checks, "stream_count": 2}
	return nil
}

func (harness *harness) applicationLifecycle() error {
	harness.recordingApplications("immediate", 4, 2)
	active := append([]appConfig(nil), harness.settings.Application...)
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", "regular", 60)
	if err != nil {
		return err
	}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
		group, err := harness.players(app, app+"-", 38)
		if err != nil {
			return err
		}
		for name, child := range group {
			players[name] = child
		}
	}
	watch := harness.watchPlayback(players)
	defer func() { watch.cancel(); <-watch.done }()
	if err := watch.waitReady(harness.ctx); err != nil {
		return fmt.Errorf("initial playback: %w", err)
	}
	if err := harness.unavailableApplication("missing", "unknown", false); err != nil {
		return err
	}
	if err := harness.rejectStreamPaths("before-reload"); err != nil {
		return err
	}
	for _, state := range []string{"disabled", "removed"} {
		for name, child := range players {
			if !child.alive() {
				return fmt.Errorf("%s ended before application %s", name, state)
			}
		}
		if state == "disabled" {
			for index := range harness.settings.Application {
				harness.settings.Application[index].Enable = false
			}
		} else {
			harness.settings.Application = []appConfig{}
		}
		if err := harness.commit(state, nil); err != nil {
			return err
		}
		latePlayers := map[string]*process{}
		for _, app := range []string{"flv", "ts"} {
			if err := harness.unavailableApplication(state, app, true); err != nil {
				return err
			}
			for protocol, location := range map[string]string{
				"http-flv": harness.origin + "/hfs/" + app + "/sample.flv",
				"hls":      harness.origin + "/hls/" + app + "/sample.m3u8",
			} {
				name := state + "-" + app + "-" + protocol
				child, err := harness.originViewer(name, location, 6)
				if err != nil {
					return err
				}
				latePlayers[name] = child
			}
		}
		if err := harness.finishPlayers(latePlayers, 140); err != nil {
			return err
		}
	}
	harness.settings.Application = append(active, appConfig{Enable: true, AppName: "added", HLS: hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 2}})
	if err := harness.commit("reenable-and-add", nil); err != nil {
		return err
	}
	if err := harness.rejectStreamPaths("after-reload"); err != nil {
		return err
	}
	for name, child := range players {
		if !child.alive() {
			return fmt.Errorf("%s ended before application reenable", name)
		}
	}
	added, err := harness.publish("publish-added", "added", "regular", 24)
	if err != nil {
		return err
	}
	if _, err := harness.waitPlaylist("added"); err != nil {
		return err
	}
	addedPlayers, err := harness.players("added", "added-", 12)
	if err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		group, err := harness.players(app, "reenabled-"+app+"-", 12)
		if err != nil {
			return err
		}
		for name, child := range group {
			addedPlayers[name] = child
		}
	}
	if err := harness.finishPlayers(addedPlayers, 275); err != nil {
		return err
	}
	if err := harness.finishPlayers(players, 925); err != nil {
		return err
	}
	if err := watch.finish(harness); err != nil {
		return err
	}
	if err := harness.finishPublishers(publishers, "publish-", 65*time.Second); err != nil {
		return err
	}
	if err := harness.finish("publish-added", added, 30*time.Second); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	if err := harness.recordingPathsUnaliased(); err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		if err := harness.recordings(filepath.Join("recordings", app), 10, true); err != nil {
			return err
		}
	}
	harness.results["application_lifecycle_verified"] = true
	return harness.checkLogs("play-*.log", "publish-*.log")
}
