package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"
)

func verifyCrashAppend(prefix, source, resumed recordingIdentity, tolerance int64) error {
	for _, track := range []string{"video", "audio"} {
		oldHashes, newHashes, actual := prefix.Video, source.Video, resumed.Video
		if track == "audio" {
			oldHashes, newHashes, actual = prefix.Audio, source.Audio, resumed.Audio
		}
		if len(oldHashes) == 0 || len(newHashes) == 0 || len(actual) != len(oldHashes)+len(newHashes) || len(resumed.Tracks[track]) != len(actual) {
			return fmt.Errorf("%s crash-resume frame count mismatch", track)
		}
		if !reflect.DeepEqual(actual[:len(oldHashes)], oldHashes) || !reflect.DeepEqual(actual[len(oldHashes):], newHashes) {
			return fmt.Errorf("%s crash-resume payload changed", track)
		}
		if len(prefix.Tracks[track]) != len(oldHashes) || len(source.Tracks[track]) != len(newHashes) {
			return errors.New("reference packet/digest count mismatch")
		}
	}
	offset := resumed.Tracks["video"][len(prefix.Video)].DTS - source.Tracks["video"][0].DTS
	for _, track := range []string{"video", "audio"} {
		boundary := len(prefix.Tracks[track])
		for index, packet := range resumed.Tracks[track] {
			if index > 0 && (packet.DTS <= resumed.Tracks[track][index-1].DTS || packet.DTS-resumed.Tracks[track][index-1].DTS > 250000) {
				return fmt.Errorf("%s crash-resume clock discontinuity", track)
			}
			if index < boundary {
				if packet != prefix.Tracks[track][index] {
					return errors.New("persisted prefix timestamps changed")
				}
				continue
			}
			expected := source.Tracks[track][index-boundary]
			allowed := int64(1000)
			if track == "audio" {
				allowed = tolerance
			}
			if absolute(packet.DTS-expected.DTS-offset) > allowed || absolute(packet.PTS-expected.PTS-offset) > allowed || packet.Key != expected.Key {
				return fmt.Errorf("%s appended timestamps differ from source", track)
			}
		}
	}
	return nil
}

func tornTSBoundary(data []byte) (int, error) {
	if len(data)%188 != 0 {
		return 0, errors.New("source TS is not packet aligned")
	}
	for position := len(data) - 188; position >= 0; position -= 188 {
		packet := data[position : position+188]
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if packet[0] != 0x47 || (pid != 0x100 && pid != 0x101) || packet[1]&0x40 == 0 || packet[3]&0x10 == 0 {
			continue
		}
		start := 4
		if packet[3]&0x20 != 0 {
			start += 1 + int(packet[4])
		}
		if start+6 > len(packet) {
			continue
		}
		payload := packet[start:]
		declared := int(payload[4])<<8 | int(payload[5])
		if bytes.Equal(payload[:3], []byte{0, 0, 1}) && declared != 0 && declared+6 > len(payload) && position+188 < len(data) {
			return position + 188, nil
		}
	}
	return 0, errors.New("source TS contains no bounded multi-packet PES to truncate")
}

func (harness *harness) recordingCrash() error {
	if remaining := 3600 - time.Now().Unix()%3600; remaining < 90 {
		return errors.New("crash scenario requires ninety seconds within one round bucket; rerun after the hour")
	}
	harness.recordingApplications("round", 3600, 2)
	if err := harness.start(); err != nil {
		return err
	}
	crashed, err := harness.publishApps("crash-publisher-", "bframes", 30)
	if err != nil {
		return err
	}
	if err := harness.sleep(7 * time.Second); err != nil {
		return err
	}
	if !harness.server.alive() || !crashed["flv"].alive() || !crashed["ts"].alive() {
		return errors.New("process ended before the planned crash")
	}
	if err := harness.server.command.Process.Kill(); err != nil {
		return err
	}
	code, err := harness.server.wait(harness.ctx, 5*time.Second)
	status, ok := harness.server.command.ProcessState.Sys().(syscall.WaitStatus)
	if err != nil || code != -1 || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		return fmt.Errorf("service did not terminate by planned SIGKILL: code=%d error=%v", code, err)
	}
	harness.results["planned_service_sigkill"] = true
	for app, child := range crashed {
		code, err := child.wait(harness.ctx, 5*time.Second)
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
	paths := map[string]string{}
	prefixes := map[string][]byte{}
	identities := map[string]recordingIdentity{}
	for _, app := range []string{"flv", "ts"} {
		files, err := recordingFiles(filepath.Join(harness.evidence, "recordings", app))
		if err != nil || len(files) != 1 {
			return fmt.Errorf("crashed %s recording files=%v error=%v", app, files, err)
		}
		paths[app] = files[0]
		prefixes[app], err = os.ReadFile(files[0])
		if err != nil {
			return err
		}
		snapshot := filepath.Join(harness.evidence, "crash-prefix."+app)
		if err := os.WriteFile(snapshot, prefixes[app], 0600); err != nil {
			return err
		}
		identity, err := inspectRecording(harness.ctx, snapshot)
		if err != nil || len(identity.Video) < 100 || len(identity.Audio) < 150 {
			return fmt.Errorf("crashed %s prefix is not independently playable: %v", app, err)
		}
		identities[app] = identity
	}
	if err := os.Rename(filepath.Join(harness.evidence, "server.log"), filepath.Join(harness.evidence, "server-crashed.log")); err != nil {
		return err
	}
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
	harness.results["append_source"] = map[string]any{"sha256": reference.SHA256, "video_frames": len(reference.Video), "audio_packets": len(reference.Audio), "audio_preroll": preroll}
	if err := harness.start(); err != nil {
		return err
	}
	appended := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		appended[app], err = harness.launch("publish-append-"+app, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-re", "-i", source, "-c", "copy", "-f", "flv", fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app))
		if err != nil {
			return err
		}
	}
	for app, child := range appended {
		if err := harness.finish("publish-append-"+app, child, 15*time.Second); err != nil {
			return err
		}
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	checks := map[string]any{}
	for _, app := range []string{"flv", "ts"} {
		files, err := recordingFiles(filepath.Join(harness.evidence, "recordings", app))
		if err != nil || len(files) != 1 || files[0] != paths[app] {
			return fmt.Errorf("crash resume changed the %s recording path: %v %v", app, files, err)
		}
		body, err := os.ReadFile(paths[app])
		if err != nil || !bytes.HasPrefix(body, prefixes[app]) {
			return fmt.Errorf("crash recovery modified persisted %s bytes: %v", app, err)
		}
		resumed, err := inspectRecording(harness.ctx, paths[app])
		if err != nil {
			return err
		}
		tolerance := int64(1000)
		if app == "ts" {
			tolerance = 10000
		}
		if err := verifyCrashAppend(identities[app], reference, resumed, tolerance); err != nil {
			return fmt.Errorf("%s: %w", app, err)
		}
		if err := os.WriteFile(filepath.Join(harness.evidence, "resumed."+app), body, 0600); err != nil {
			return err
		}
		checks[app] = map[string]any{"prefix_bytes": len(prefixes[app]), "prefix_sha256": identities[app].SHA256, "resumed_sha256": resumed.SHA256, "video_frames": len(resumed.Video), "audio_packets": len(resumed.Audio), "same_path": paths[app], "prefix_unchanged": true, "appended_source_identity": true}
		boundary := len(body) - 1
		if app == "ts" {
			boundary, err = tornTSBoundary(body)
			if err != nil {
				return err
			}
		}
		prefixes[app] = bytes.Clone(body[:boundary])
		if err := os.WriteFile(paths[app], prefixes[app], 0600); err != nil {
			return err
		}
	}
	harness.results["crash_resume_identity"] = checks
	if err := os.Rename(filepath.Join(harness.evidence, "server.log"), filepath.Join(harness.evidence, "server-resumed.log")); err != nil {
		return err
	}
	return harness.tornRecordingMedia(paths, prefixes)
}

func (harness *harness) tornRecordingMedia(paths map[string]string, damaged map[string][]byte) error {
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-recovery-", "regular", 30)
	if err != nil {
		return err
	}
	players := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
		group, err := harness.players(app, "recovery-"+app+"-", 18)
		if err != nil {
			return err
		}
		maps.Copy(players, group)
	}
	watch := harness.watchPlayback(players)
	defer func() { watch.cancel(); <-watch.done }()
	if err := watch.waitReady(harness.ctx); err != nil {
		return fmt.Errorf("torn recording playback warmup: %w", err)
	}
	for _, app := range []string{"flv", "ts"} {
		state, err := harness.waitRecording("torn "+app+" file rejection", app, 5*time.Second, func(state componentStatus) bool {
			return state.State == "failed" && state.Failures > 0 && state.LastFailureAt > 0
		})
		if err != nil {
			return err
		}
		harness.results["torn_recording_failure-"+app] = state
	}
	for index := range harness.settings.Application {
		recorder := &harness.settings.Application[index].Recorder
		recorder.SaveTo = filepath.Join(harness.evidence, "recovered")
		recorder.SaveMode, recorder.Segment = "immediate", 4
	}
	if err := harness.commit("recover-torn-recordings-to-new-directory", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		state, err := harness.recordingStatus(app)
		if err != nil || state.State != "active" || state.Failures == 0 || state.LastFailureAt == 0 || state.Revision != 2 {
			return fmt.Errorf("recovered %s status lost its failure history: %+v %v", app, state, err)
		}
		harness.results["torn_recovered_status-"+app] = state
	}
	if err := harness.finishPlayers(players, 425); err != nil {
		return err
	}
	if err := watch.finish(harness); err != nil {
		return err
	}
	if err := harness.finishPublishers(publishers, "publish-recovery-", 35*time.Second); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		body, err := os.ReadFile(paths[app])
		if err != nil || !bytes.Equal(body, damaged[app]) {
			return fmt.Errorf("rejected %s file changed: %v", app, err)
		}
		harness.results["damaged_file_preserved-"+app] = map[string]any{"bytes": len(body), "sha256": fmt.Sprintf("%x", sha256.Sum256(body))}
		if err := harness.recordings(filepath.Join("recovered", app), 4, true); err != nil {
			return err
		}
	}
	logs := map[string]any{}
	for _, name := range []string{"server-crashed.log", "server-resumed.log", "server.log"} {
		body, err := os.ReadFile(filepath.Join(harness.evidence, name))
		if err != nil {
			return err
		}
		events, err := validateCrashServiceLog(body, name == "server.log")
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		logs[name] = events
	}
	harness.results["classified_service_events"] = logs
	harness.results["recording_crash_verified"] = true
	return harness.checkLogs("play-*.log", "publish-*.log")
}

func validateCrashServiceLog(body []byte, damaged bool) (map[string]int, error) {
	counts := map[string]int{}
	allowed := map[string]bool{}
	for _, entry := range [][2]string{{"flv", "invalid existing FLV tail length"}, {"ts", "existing TS PES length does not match recorded payload"}} {
		allowed[entry[1]] = true
		allowed["["+entry[0]+"/sample]recording failed: "+entry[1]] = true
		allowed["close failed,sync=false,block=true: "+entry[1]] = true
	}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		var event struct {
			Level   string `json:"level"`
			Message string `json:"msg"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, err
		}
		if event.Level == "ERROR" && (!damaged || !allowed[event.Message]) {
			return nil, fmt.Errorf("unexpected service failure: %q", event.Message)
		}
		if event.Level == "WARN" && event.Message != "Not continuous timestamp" {
			return nil, fmt.Errorf("unknown service warning: %q", event.Message)
		}
		if event.Level == "ERROR" || event.Level == "WARN" {
			counts[event.Message]++
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if damaged {
		for message := range allowed {
			if counts[message] == 0 {
				return nil, fmt.Errorf("missing injected file failure: %q", message)
			}
		}
	}
	return counts, nil
}
