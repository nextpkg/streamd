package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/nextpkg/streamd/internal/objectstore"
)

type crashCloudState struct {
	Uploads     []objectstore.Upload          `json:"uploads"`
	Parts       map[string][]objectstore.Part `json:"parts"`
	Checkpoints map[string]string             `json:"checkpoints"`
}

func (harness *harness) cloudCrashSnapshot(backend *storageFaultBackend, client *objectstore.Client) (crashCloudState, map[string][]byte, error) {
	state := crashCloudState{Parts: map[string][]objectstore.Part{}}
	uploads, err := client.Backend.Uploads(harness.ctx, "")
	if err != nil {
		return state, nil, err
	}
	state.Uploads = uploads
	sort.Slice(state.Uploads, func(left, right int) bool { return state.Uploads[left].Key < state.Uploads[right].Key })
	for _, upload := range uploads {
		parts, err := client.Backend.Parts(harness.ctx, upload.Key, upload.ID)
		if err != nil {
			return state, nil, err
		}
		state.Parts[upload.Key] = parts
	}
	objects, err := harness.completionObjects(backend.reader, backend.contract)
	if err != nil {
		return state, nil, err
	}
	state.Checkpoints = byteDigests(objects)
	return state, objects, nil
}

func validateCloudCrashState(mode string, seed, crashed crashCloudState) error {
	if len(crashed.Uploads) != 2 || len(crashed.Parts) != 2 {
		return errors.New("crash requires two real multipart uploads")
	}
	if mode == "missing" {
		if len(crashed.Checkpoints) != 0 {
			return errors.New("missing-checkpoint fixture unexpectedly persisted state")
		}
	} else if mode == "confirmed" || mode == "stale" {
		if len(seed.Checkpoints) != 2 || !reflect.DeepEqual(seed.Checkpoints, crashed.Checkpoints) || !reflect.DeepEqual(seed.Uploads, crashed.Uploads) {
			return errors.New("crash changed the confirmed checkpoint or upload identity")
		}
	} else {
		return errors.New("unknown cloud crash state")
	}
	for _, upload := range crashed.Uploads {
		parts := crashed.Parts[upload.Key]
		if mode == "confirmed" {
			if !reflect.DeepEqual(seed.Parts[upload.Key], parts) {
				return errors.New("confirmed checkpoint has advanced remote parts")
			}
		} else if len(parts) == 0 || (mode == "stale" && reflect.DeepEqual(seed.Parts[upload.Key], parts)) {
			return errors.New("unsafe fixture never persisted an uncheckpointed full part")
		}
	}
	return nil
}

func (harness *harness) cloudCrash() (result error) {
	mode := os.Getenv("STREAMD_E2E_CRASH_STATE")
	if mode == "" {
		mode = "confirmed"
	}
	if mode != "confirmed" && mode != "missing" && mode != "stale" {
		return errors.New("STREAMD_E2E_CRASH_STATE must be confirmed, missing or stale")
	}
	if remaining := 86400 - time.Now().Unix()%86400; remaining < 180 {
		return errors.New("cloud crash needs three minutes within its day bucket")
	}
	backend, err := harness.startStorageFaultBackend()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, backend.close(harness)) }()
	var client *objectstore.Client
	if backend.provider == "aws" {
		client, err = objectstore.NewS3(&objectstore.S3Config{Enable: true, Endpoint: backend.endpoint, Bucket: backend.bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: true})
	} else {
		client, err = objectstore.NewCOS(&objectstore.COSConfig{Enable: true, Endpoint: backend.endpoint, SecretID: "test-id", SecretKey: "test-secret"})
	}
	if err != nil {
		return err
	}
	defer client.Close()
	harness.recordingApplications("round", 86400, 2)
	for index := range harness.settings.Application {
		recorder := &harness.settings.Application[index].Recorder
		backend.configure(recorder, backend.endpoint)
		recorder.Segment = 86400
	}
	callbacks := harness.storageCallbacks()
	defer callbacks.server.Close()
	defer func() {
		observations, invalid := callbacks.receiver.snapshot()
		harness.results["cloud_crash_callbacks"], harness.results["cloud_crash_invalid_callback"] = observations, invalid
	}()
	harness.results["cloud_crash_state"], harness.results["changes"] = mode, []map[string]any{}
	if err := harness.start(); err != nil {
		return err
	}
	defer harness.cleanup()
	harness.results["cloud_crash_started_ms"] = harness.started.UnixMilli()
	seedState := crashCloudState{}
	prefixes := map[string][]byte{}
	if mode != "missing" {
		seed, err := harness.publishApps("seed-", "bframes", 5)
		if err != nil {
			return err
		}
		for app, publisher := range seed {
			if err := harness.finish("seed-"+app, publisher, 15*time.Second); err != nil {
				return err
			}
		}
		if err := harness.cloudCrashRetired(); err != nil {
			return err
		}
		var objects map[string][]byte
		seedState, objects, err = harness.cloudCrashSnapshot(backend, client)
		if err != nil {
			return err
		}
		if len(objects) != 2 {
			return errors.New("seed did not persist two checkpoints")
		}
		for filename, body := range objects {
			var saved struct {
				Key   string             `json:"key"`
				Tail  []byte             `json:"tail"`
				Parts []objectstore.Part `json:"parts"`
			}
			if !strings.Contains(filename, "/.streamd-checkpoints/") || json.Unmarshal(body, &saved) != nil || len(saved.Tail) == 0 || len(saved.Parts) != 0 {
				return errors.New("seed must fit entirely in a durable short-tail checkpoint")
			}
			app := strings.TrimPrefix(filepath.Ext(saved.Key), ".")
			if app != "flv" && app != "ts" {
				return errors.New("invalid checkpoint media key")
			}
			prefixes[app] = saved.Tail
			if err := os.WriteFile(filepath.Join(harness.evidence, "checkpoint-prefix."+app), saved.Tail, 0600); err != nil {
				return err
			}
		}
	}
	fixture := filepath.Join(harness.root, "testdata/generated/bframes.flv")
	if mode != "confirmed" {
		fixture = filepath.Join(harness.evidence, "high-bitrate.flv")
		_, err := mediaCommand(harness.ctx, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-i", filepath.Join(harness.root, "testdata/generated/bframes.flv"), "-t", "12", "-vf", "scale=1280:720,noise=alls=24:allf=t+u:all_seed=1", "-c:a", "copy", "-c:v", "libx264", "-preset", "ultrafast", "-g", "25", "-bf", "2", "-b:v", "12M", "-minrate", "12M", "-maxrate", "12M", "-bufsize", "12M", "-x264-params", "nal-hrd=cbr:force-cfr=1", fixture)
		if err != nil {
			return err
		}
	}
	crashing := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		crashing[app], err = harness.launch("crash-publisher-"+app, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-re", "-stream_loop", "-1", "-i", fixture, "-t", "30", "-c", "copy", "-f", "flv", fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app))
		if err != nil {
			return err
		}
	}
	if mode == "confirmed" {
		if err := harness.sleep(5 * time.Second); err != nil {
			return err
		}
	} else if err := harness.until("both cloud recordings uploaded a full uncheckpointed part", 15*time.Second, func() (bool, error) {
		state, _, err := harness.cloudCrashSnapshot(backend, client)
		if err != nil {
			return false, err
		}
		return validateCloudCrashState(mode, seedState, state) == nil, nil
	}); err != nil {
		return err
	}
	if !harness.server.alive() || !crashing["flv"].alive() || !crashing["ts"].alive() {
		return errors.New("crash target exited before SIGKILL")
	}
	if err := harness.server.command.Process.Kill(); err != nil {
		return err
	}
	code, err := harness.server.wait(harness.ctx, 5*time.Second)
	status, ok := harness.server.command.ProcessState.Sys().(syscall.WaitStatus)
	if err != nil || code != -1 || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		return errors.New("service was not terminated by SIGKILL")
	}
	harness.results["planned_service_sigkill"] = true
	for app, publisher := range crashing {
		code, err := publisher.wait(harness.ctx, 5*time.Second)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(harness.evidence, "crash-publisher-"+app+".log"))
		if err != nil {
			return err
		}
		if err := validateShutdownPublisher(code, string(body)); err != nil {
			return err
		}
		harness.results["planned_publisher_interruption-"+app] = code
	}
	crashedState, _, err := harness.cloudCrashSnapshot(backend, client)
	if err != nil {
		return err
	}
	if err := validateCloudCrashState(mode, seedState, crashedState); err != nil {
		return err
	}
	harness.results["cloud_crash_seed"], harness.results["cloud_crash_persisted"] = seedState, crashedState
	if err := os.Rename(filepath.Join(harness.evidence, "server.log"), filepath.Join(harness.evidence, "server-crashed.log")); err != nil {
		return err
	}
	if err := harness.start(); err != nil {
		return err
	}
	if mode == "confirmed" {
		if err := harness.cloudCrashConfirmed(backend, client, prefixes, crashedState); err != nil {
			return err
		}
	}
	return harness.cloudCrashLiveRecovery(backend, client, callbacks, mode, crashedState)
}

func (harness *harness) cloudCrashRetired() error {
	return harness.until("cloud recording terminals retired", 10*time.Second, func() (bool, error) {
		report, err := harness.configuration()
		for _, component := range report.Components {
			if component.Component == "recording" {
				return false, err
			}
		}
		return err == nil, err
	})
}

func (harness *harness) cloudCrashConfirmed(backend *storageFaultBackend, client *objectstore.Client, prefixes map[string][]byte, crashed crashCloudState) error {
	source := filepath.Join(harness.evidence, "append-source.flv")
	if _, err := mediaCommand(harness.ctx, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-i", filepath.Join(harness.root, "testdata/generated/regular.flv"), "-t", "8", "-c", "copy", source); err != nil {
		return err
	}
	reference, err := inspectRecording(harness.ctx, source)
	if err != nil {
		return err
	}
	input, err := harness.probePackets(source)
	if err != nil {
		return err
	}
	preroll := 0
	for _, packet := range input.Packets {
		if packet.CodecType == "video" && strings.Contains(packet.Flags, "K") {
			break
		}
		if packet.CodecType == "audio" {
			preroll++
		}
	}
	reference.Audio, reference.Tracks["audio"] = reference.Audio[preroll:], reference.Tracks["audio"][preroll:]
	publishers := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		publishers[app], err = harness.launch("append-"+app, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-re", "-i", source, "-c", "copy", "-f", "flv", fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app))
		if err != nil {
			return err
		}
	}
	for app, publisher := range publishers {
		if err := harness.finish("append-"+app, publisher, 15*time.Second); err != nil {
			return err
		}
	}
	if err := harness.cloudCrashRetired(); err != nil {
		return err
	}
	state, objects, err := harness.cloudCrashSnapshot(backend, client)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(state.Uploads, crashed.Uploads) || len(objects) != 2 {
		return errors.New("confirmed recovery replaced multipart identity")
	}
	verified := map[string]any{}
	for _, body := range objects {
		var saved struct {
			Key   string             `json:"key"`
			Tail  []byte             `json:"tail"`
			Parts []objectstore.Part `json:"parts"`
		}
		if json.Unmarshal(body, &saved) != nil || len(saved.Parts) != 0 {
			return errors.New("unexpected resumed checkpoint layout")
		}
		app := strings.TrimPrefix(filepath.Ext(saved.Key), ".")
		if len(prefixes[app]) == 0 || !bytes.HasPrefix(saved.Tail, prefixes[app]) {
			return errors.New("confirmed recovery changed persisted media prefix")
		}
		filename := filepath.Join(harness.evidence, "checkpoint-resumed."+app)
		if err := os.WriteFile(filename, saved.Tail, 0600); err != nil {
			return err
		}
		prefix, err := inspectRecording(harness.ctx, filepath.Join(harness.evidence, "checkpoint-prefix."+app))
		if err != nil {
			return err
		}
		resumed, err := inspectRecording(harness.ctx, filename)
		if err != nil {
			return err
		}
		tolerance := int64(1000)
		if app == "ts" {
			tolerance = 10000
		}
		if err := verifyCrashAppend(prefix, reference, resumed, tolerance); err != nil {
			return fmt.Errorf("confirmed %s: %w", app, err)
		}
		verified[app] = map[string]any{"prefix_sha256": prefix.SHA256, "resumed_sha256": resumed.SHA256, "prefix_bytes": len(prefixes[app]), "video_frames": len(resumed.Video), "audio_packets": len(resumed.Audio), "unconfirmed_tail_not_replayed": true}
	}
	if len(verified) != 2 {
		return errors.New("missing confirmed recovery format")
	}
	harness.results["cloud_crash_confirmed_identity"] = verified
	return nil
}

func (harness *harness) cloudCrashLiveRecovery(backend *storageFaultBackend, client *objectstore.Client, callbacks *storageFaultCallbacks, mode string, crashed crashCloudState) (result error) {
	if mode == "confirmed" {
		for index := range harness.settings.Application {
			app := &harness.settings.Application[index]
			app.Recorder = recorderConfig{Enable: true, Driver: "fs", SaveAs: app.AppName, SaveMode: "immediate", Segment: 4, SaveTo: filepath.Join(harness.evidence, "recovered"), Event: callbacks.recordingEvent("recovered")}
		}
		if err := harness.commit("confirmed restart recovered; use filesystem for continuous live check", nil); err != nil {
			return err
		}
	}
	liveStarted := time.Now().UnixMilli()
	publishers, err := harness.publishApps("publish-", "bframes", 45)
	if err != nil {
		return err
	}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
		group, err := harness.players(app, app+"-", 30)
		if err != nil {
			return err
		}
		maps.Copy(players, group)
	}
	watch := harness.watchPlayback(players)
	defer func() { result = errors.Join(result, watch.finish(harness)) }()
	select {
	case <-watch.ready:
	case <-watch.done:
		return fmt.Errorf("cloud crash playback warmup: %w", watch.err)
	case <-harness.ctx.Done():
		return harness.ctx.Err()
	}
	if mode != "confirmed" {
		if err := harness.until("unsafe cloud resume visibly rejected while live playback continues", 8*time.Second, func() (bool, error) {
			report, err := harness.configuration()
			failed := 0
			for _, component := range report.Components {
				if component.Component == "recording" && component.State == "failed" && component.Failures > 0 && component.LastFailureAt > 0 {
					failed++
				}
			}
			return failed == 2, err
		}); err != nil {
			return err
		}
		current, _, err := harness.cloudCrashSnapshot(backend, client)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, crashed) {
			return errors.New("unsafe restart modified persisted upload or checkpoint")
		}
		for index := range harness.settings.Application {
			app := &harness.settings.Application[index]
			app.Recorder = recorderConfig{Enable: true, Driver: "fs", SaveAs: app.AppName, SaveMode: "immediate", Segment: 4, SaveTo: filepath.Join(harness.evidence, "recovered"), Event: callbacks.recordingEvent("recovered")}
		}
		if err := harness.commit("recover recording after unsafe cloud state is rejected", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
			return err
		}
	}
	if err := harness.finishPlayers(players, 675); err != nil {
		return err
	}
	for app, publisher := range publishers {
		if err := harness.finish("publish-"+app, publisher, 20*time.Second); err != nil {
			return err
		}
	}
	if err := harness.cloudCrashRetired(); err != nil {
		return err
	}
	if err := harness.until("crash callbacks match interrupted, resumed and recovered media", 5*time.Second, func() (bool, error) {
		observations, invalid := callbacks.receiver.snapshot()
		if invalid != "" {
			return false, errors.New(invalid)
		}
		files, err := recordingFiles(filepath.Join(harness.evidence, "recovered"))
		if err != nil {
			return false, err
		}
		identities := []string{}
		for _, filename := range files {
			relative, err := filepath.Rel(harness.evidence, filename)
			if err != nil {
				return false, err
			}
			identities = append(identities, filepath.ToSlash(relative))
		}
		counts, err := validateCloudCrashCallbacks(observations, identities, mode, harness.results["cloud_crash_started_ms"].(int64), liveStarted, crashed.Uploads)
		if err == nil {
			harness.results["cloud_crash_callback_counts"] = counts
		}
		return err == nil, nil
	}); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	if err := harness.recordings("recovered", 12, true); err != nil {
		return err
	}
	current, _, err := harness.cloudCrashSnapshot(backend, client)
	if err != nil {
		return err
	}
	if mode != "confirmed" && !reflect.DeepEqual(current, crashed) {
		return errors.New("recovery or shutdown modified the rejected cloud state")
	}
	observations, invalid := callbacks.receiver.snapshot()
	if invalid != "" {
		return errors.New(invalid)
	}
	for _, observation := range observations {
		if observation.Path == "/initial" && observation.Payload["event"] == "OnRecordDone" {
			return errors.New("interrupted or paused cloud recording falsely reported complete")
		}
	}
	if err := harness.checkLogs("seed-*.log", "append-*.log", "publish-*.log", "play-*.log"); err != nil {
		return err
	}
	if err := harness.cloudCrashErrors(mode); err != nil {
		return err
	}
	harness.results["cloud_crash_preserved_after_recovery"] = current
	harness.results["cloud_crash_verified"] = true
	return nil
}
