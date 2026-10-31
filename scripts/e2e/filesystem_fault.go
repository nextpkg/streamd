package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (harness *harness) filesystemFault() (result error) {
	mode := os.Getenv("STREAMD_E2E_FILESYSTEM_FAULT")
	if mode == "" {
		mode = "permission"
	}
	if mode != "permission" && mode != "full" {
		return fmt.Errorf("unsupported filesystem fault %q", mode)
	}
	faultApp, healthyApp, err := storageFaultApplications()
	if err != nil {
		return err
	}
	harness.recordingApplications("immediate", 4, 2)
	if faultApp == "ts" {
		harness.settings.Application[0], harness.settings.Application[1] = harness.settings.Application[1], harness.settings.Application[0]
	}
	callbacks := harness.filesystemCallbacks()
	defer func() {
		harness.results["filesystem_callbacks"], harness.results["filesystem_callback_invalid"] = callbacks.receiver.snapshot()
		callbacks.server.Close()
	}()
	harness.results["fault_mode"] = "filesystem-" + mode
	harness.results["fault_application"] = faultApp
	harness.results["fault_scope"] = "actual Linux non-root EACCES or ENOSPC in isolated size-limited tmpfs; host filesystem permissions and capacity are untouched"
	container, err := harness.startFilesystemContainer()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, container.close(harness)) }()
	defer harness.cleanup()
	if body, err := container.execute(harness, "wget", "-q", "-O", "-", callbacks.server.URL+"/ready"); err != nil || string(body) != "ready" {
		return fmt.Errorf("container callback route unavailable: %v: %s", err, body)
	}
	firstClock, err := container.sampleClock(harness)
	if err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	players, watch, err := harness.faultPlayers()
	if watch != nil {
		defer func() { result = errors.Join(result, watch.finish(harness)) }()
	}
	if err != nil {
		return err
	}
	directory, expectedCause := "/fault/"+faultApp+"/sample", "permission denied"
	var probe []byte
	if mode == "permission" {
		if _, err := container.execute(harness, "chmod", "000", directory); err != nil {
			return err
		}
		probe, err = container.execute(harness, "touch", directory+"/permission-probe")
	} else {
		expectedCause = "no space left on device"
		probe, err = container.execute(harness, "dd", "if=/dev/zero", "of=/fault/capacity-fill", "bs=1048576", "count=16")
	}
	if err == nil || !strings.Contains(strings.ToLower(string(probe)), expectedCause) {
		return fmt.Errorf("filesystem fault was not enforced: %v: %s", err, probe)
	}
	harness.results["filesystem_probe"] = string(probe)
	if mode == "full" {
		body, err := container.execute(harness, "stat", "-c", "%s", "/fault/capacity-fill")
		if err != nil {
			return err
		}
		size, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
		if err != nil || size <= 4096 {
			return fmt.Errorf("invalid capacity filler extent: %s", body)
		}
		if _, err := container.execute(harness, "truncate", "-s", strconv.FormatInt(size-4096, 10), "/fault/capacity-fill"); err != nil {
			return err
		}
		harness.results["released_after_fill_bytes"] = 4096
	}
	harness.results["fault_started_seconds"] = time.Since(harness.started).Seconds()
	failed, err := harness.waitRecording("filesystem failure visible", faultApp, 8*time.Second, func(state componentStatus) bool {
		return state.State == "failed" && state.Failures > 0 && state.LastFailureAt > 0
	})
	if err != nil {
		return err
	}
	harness.results["failed_recording_status"] = failed
	if err := harness.sleep(3 * time.Second); err != nil {
		return err
	}
	peer, err := harness.recordingStatus(healthyApp)
	if err != nil || peer.State != "active" || peer.Failures != 0 {
		return fmt.Errorf("healthy recorder was affected: %+v: %v", peer, err)
	}
	harness.results["healthy_peer_during_fault"] = peer
	if mode == "permission" {
		_, err = container.execute(harness, "chmod", "700", directory)
	} else {
		_, err = container.execute(harness, "rm", "/fault/capacity-fill")
	}
	if err != nil {
		return err
	}
	recovered, err := harness.waitRecording("automatic filesystem recovery without configuration change", faultApp, 5*time.Second, func(state componentStatus) bool {
		return state.State == "active" && state.Revision == failed.Revision && state.Failures >= failed.Failures && state.LastFailureAt > 0
	})
	if err != nil {
		return err
	}
	harness.results["recovered_recording_status"] = recovered
	harness.results["recovery_seconds"] = time.Since(harness.started).Seconds()
	if err := harness.finishPlayers(players, 46*25*9/10); err != nil {
		return err
	}
	if err := harness.finishPublishers(publishers, "publish-", 75*time.Second); err != nil {
		return err
	}
	if err := harness.until("recorders released", 5*time.Second, func() (bool, error) {
		report, err := harness.configuration()
		if err != nil {
			return false, err
		}
		for _, state := range report.Components {
			if state.Component == "recording" {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	lastClock, err := container.sampleClock(harness)
	if err != nil {
		return err
	}
	if err := container.collect(harness); err != nil {
		return err
	}
	if err := harness.recordings("recordings", 20, true); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	expected, err := faultErrorsForApp(body, faultApp, func(message string) bool {
		return strings.Contains(message, directory) && strings.Contains(message, expectedCause)
	})
	harness.results["expected_server_errors"] = expected
	if err != nil {
		return err
	}
	if mode == "full" {
		files, err := verifyFilesystemRollback(harness.evidence, expected)
		if err != nil {
			return err
		}
		harness.results["filesystem_rollback_files"] = files
		harness.results["partial_write_rollback_verified"] = true
	}
	if err := callbacks.verifyFilesystem(harness, faultApp, mode, expected, []filesystemClockSample{firstClock, lastClock}); err != nil {
		return err
	}
	return harness.checkLogs("publish-*.log", "play-*.log")
}

func verifyFilesystemRollback(directory string, messages []string) ([]map[string]any, error) {
	pattern := regexp.MustCompile(`recording append failed: written=([0-9]+) requested=([0-9]+) rollback_offset=([0-9]+): write (/fault/(?:flv|ts)/sample/[0-9]+\.(?:flv|ts)): no space left on device`)
	seen := make(map[string]bool)
	var files []map[string]any
	for _, message := range messages {
		match := pattern.FindStringSubmatch(message)
		if match == nil || seen[match[4]] {
			continue
		}
		written, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil || written <= 0 {
			return nil, fmt.Errorf("invalid partial-write extent %q", match[1])
		}
		requested, err := strconv.ParseInt(match[2], 10, 64)
		if err != nil || requested <= written {
			return nil, fmt.Errorf("invalid partial-write request extent %q", match[2])
		}
		offset, err := strconv.ParseInt(match[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid rollback extent %q: %w", match[3], err)
		}
		relative := filepath.Join("recordings", strings.TrimPrefix(match[4], "/"))
		info, err := os.Stat(filepath.Join(directory, relative))
		if offset == 0 {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("failed new recording was not removed: %s: %v", relative, err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("confirmed recording prefix was lost: %s: %w", relative, err)
		} else if !info.Mode().IsRegular() || info.Size() != offset {
			return nil, fmt.Errorf("recording rollback extent mismatch: %s: got %d want %d", relative, info.Size(), offset)
		}
		seen[match[4]] = true
		files = append(files, map[string]any{"file": relative, "accepted_bytes": offset, "partial_write_bytes": written, "requested_bytes": requested, "removed_empty": offset == 0})
	}
	if len(files) == 0 {
		return nil, errors.New("disk-full test did not exercise an actual partial media write")
	}
	return files, nil
}
