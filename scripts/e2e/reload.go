package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type componentKey struct {
	component string
	stream    string
}

func componentsApplied(report configurationReport, revision int, expected map[componentKey]string) bool {
	if report.Revision != revision {
		return false
	}
	states := make(map[componentKey]componentStatus)
	for _, component := range report.Components {
		states[componentKey{component.Component, component.Stream}] = component
	}
	for key, state := range expected {
		component, exists := states[key]
		if !exists || component.Revision != revision || component.State != state {
			return false
		}
	}
	return true
}

func (harness *harness) commit(label string, expected map[componentKey]string) error {
	previous, err := harness.configuration()
	if err != nil {
		return err
	}
	if err := harness.atomicSettings(); err != nil {
		return err
	}
	var snapshot configurationReport
	if err := harness.until("keyframe application "+label, 10*time.Second, func() (bool, error) {
		var err error
		snapshot, err = harness.configuration()
		return componentsApplied(snapshot, previous.Revision+1, expected), err
	}); err != nil {
		return err
	}
	changes, _ := harness.results["changes"].([]map[string]any)
	harness.results["changes"] = append(changes, map[string]any{"label": label, "elapsed": time.Since(harness.started).Seconds(),
		"revision": snapshot.Revision, "components": snapshot.Components})
	fmt.Printf("%s: revision %d applied\n", label, snapshot.Revision)
	return nil
}

func (harness *harness) captureSegment() (string, []byte, error) {
	playlist, err := harness.playlist("live")
	if err != nil {
		return "", nil, err
	}
	location, err := segmentURL(harness.origin, "live", playlist)
	if err != nil {
		return "", nil, err
	}
	status, body, err := harness.fetch(location)
	if err != nil {
		return "", nil, err
	}
	if status != http.StatusOK || len(body) == 0 {
		return "", nil, fmt.Errorf("capture segment: HTTP %d, bytes=%d", status, len(body))
	}
	return location, body, nil
}

func (harness *harness) requirePreserved(location string, payload []byte, result string) error {
	status, body, err := harness.fetch(location)
	if err != nil {
		return err
	}
	if status != http.StatusOK || !bytes.Equal(body, payload) {
		return fmt.Errorf("%s: issued URL changed after reload, HTTP %d", result, status)
	}
	harness.results[result] = true
	return nil
}

func signatureURLs(location, stream, key string, expires int64) (string, string, error) {
	parsed, err := url.Parse(location)
	if err != nil {
		return "", "", err
	}
	query := parsed.Query()
	if query.Get("token") == "" || query.Get("e") == "" {
		return "", "", errors.New("segment URL has no signature")
	}
	query.Set("token", "forged")
	parsed.RawQuery = query.Encode()
	invalid := parsed.String()
	digest := md5.Sum([]byte(fmt.Sprintf("%s %s %d", stream, key, expires)))
	query.Set("token", base64.RawURLEncoding.EncodeToString(digest[:]))
	query.Set("e", strconv.FormatInt(expires, 10))
	parsed.RawQuery = query.Encode()
	return invalid, parsed.String(), nil
}

func (harness *harness) signatures() error {
	location, _, err := harness.captureSegment()
	if err != nil {
		return err
	}
	invalid, expired, err := signatureURLs(location, "live/sample", "test-third-key", time.Now().Unix()-1)
	if err != nil {
		return err
	}
	for location, expected := range map[string]int{invalid: http.StatusForbidden, expired: http.StatusGone} {
		status, _, err := harness.fetch(location)
		if err != nil {
			return err
		}
		if status != expected {
			return fmt.Errorf("signature returned HTTP %d, want %d", status, expected)
		}
	}
	harness.results["signature_rejections"] = map[string]int{"invalid": http.StatusForbidden, "expired": http.StatusGone}
	return nil
}

func (harness *harness) reload() error {
	harness.settings.Application = []appConfig{
		{Enable: true, AppName: "live", HLS: hlsConfig{Enable: true, Fragment: 2, PlaylistLength: 2, SecureKey: "test-old-key", SecureTTL: 30}},
		{Enable: true, AppName: "toggle", HLS: hlsConfig{Fragment: 2, PlaylistLength: 2}},
	}
	harness.results["changes"] = []map[string]any{}
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	initial, err := harness.waitPlaylist("live")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(harness.evidence, "initial.m3u8"), []byte(initial), 0600); err != nil {
		return err
	}
	status, _, err := harness.fetch("/hls/toggle/sample.m3u8")
	if err != nil {
		return err
	}
	if status != http.StatusNotFound {
		return fmt.Errorf("disabled HLS returned HTTP %d", status)
	}
	players, err := harness.players("live", "", 44)
	if err != nil {
		return err
	}
	active := map[componentKey]string{{"hls", "live/sample"}: "active", {"recording", "live/sample"}: "active", {"hls", "toggle/sample"}: "active"}
	live, toggle := &harness.settings.Application[0], &harness.settings.Application[1]
	if err := harness.waitAt(9); err != nil {
		return err
	}
	oldURL, oldPayload, err := harness.captureSegment()
	if err != nil {
		return err
	}
	live.HLS.Fragment, live.HLS.PlaylistLength, live.HLS.SecureKey = 4, 4, "test-new-key"
	live.Recorder = recorderConfig{Enable: true, SaveAs: "flv", SaveMode: "immediate", Segment: 4, Driver: "fs", SaveTo: filepath.Join(harness.evidence, "recordings-a")}
	toggle.HLS.Enable = true
	if err := harness.commit("enable-flv-grow-hls-rotate-key", active); err != nil {
		return err
	}
	if err := harness.requirePreserved(oldURL, oldPayload, "old_signed_url_preserved"); err != nil {
		return err
	}
	if _, err := harness.waitPlaylist("toggle"); err != nil {
		return err
	}
	if err := harness.waitAt(19); err != nil {
		return err
	}
	oldURL, oldPayload, err = harness.captureSegment()
	if err != nil {
		return err
	}
	live.HLS.Fragment, live.HLS.PlaylistLength, live.HLS.SecureKey = 2, 2, "test-third-key"
	live.Recorder.SaveAs, live.Recorder.SaveTo = "ts", filepath.Join(harness.evidence, "recordings-b")
	if err := harness.commit("switch-ts-path-shrink-hls-rotate-key", active); err != nil {
		return err
	}
	if err := harness.requirePreserved(oldURL, oldPayload, "old_segment_after_shrink_preserved"); err != nil {
		return err
	}
	if err := harness.signatures(); err != nil {
		return err
	}
	if err := harness.waitAt(29); err != nil {
		return err
	}
	live.Recorder.Enable, toggle.HLS.Enable = false, false
	disabled := maps.Clone(active)
	disabled[componentKey{"recording", "live/sample"}], disabled[componentKey{"hls", "toggle/sample"}] = "disabled", "disabled"
	if err := harness.commit("disable-recording-and-secondary-hls", disabled); err != nil {
		return err
	}
	ended, err := harness.playlist("toggle")
	if err != nil {
		return err
	}
	if !strings.Contains(ended, "#EXT-X-ENDLIST") {
		return errors.New("disabled HLS did not end its playlist")
	}
	before, err := harness.recordingSizes()
	if err != nil {
		return err
	}
	if err := harness.sleep(time.Second); err != nil {
		return err
	}
	after, err := harness.recordingSizes()
	if err != nil {
		return err
	}
	if len(before) == 0 || !maps.Equal(before, after) {
		return errors.New("disabled recordings absent or still growing")
	}
	harness.results["disabled_recordings_stopped"] = true
	if err := harness.waitAt(35); err != nil {
		return err
	}
	live.Recorder.Enable, live.Recorder.SaveAs, live.Recorder.SaveTo = true, "flv", filepath.Join(harness.evidence, "recordings-c")
	toggle.HLS.Enable = true
	if err := harness.commit("reenable-recording-and-secondary-hls", active); err != nil {
		return err
	}
	var resumed string
	if err := harness.until("HLS discontinuity after resuming", 10*time.Second, func() (bool, error) {
		var err error
		resumed, err = harness.playlist("toggle")
		return strings.Contains(resumed, "#EXT-X-DISCONTINUITY") && !strings.Contains(resumed, "#EXT-X-ENDLIST"), err
	}); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(harness.evidence, "resumed.m3u8"), []byte(resumed), 0600); err != nil {
		return err
	}
	if err := harness.waitAt(43); err != nil {
		return err
	}
	oldURL, oldPayload, err = harness.captureSegment()
	if err != nil {
		return err
	}
	live.HLS.SecureTTL = 20
	if err := harness.commit("reduce-signature-ttl-without-changing-key", active); err != nil {
		return err
	}
	if err := harness.requirePreserved(oldURL, oldPayload, "ttl_reduction_preserves_issued_url"); err != nil {
		return err
	}
	previous, err := harness.configuration()
	if err != nil {
		return err
	}
	compact, err := json.Marshal(harness.settings)
	if err != nil {
		return err
	}
	if err := harness.atomicConfiguration(compact); err != nil {
		return err
	}
	if err := harness.sleep(500 * time.Millisecond); err != nil {
		return err
	}
	current, err := harness.configuration()
	if err != nil {
		return err
	}
	if current.Revision != previous.Revision {
		return errors.New("identical configuration changed its revision")
	}
	harness.results["identical_config_noop"] = true
	if err := harness.finishPlayers(players, 1000); err != nil {
		return err
	}
	if err := harness.finishPublishers(publishers, "publish-", 65*time.Second); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, directory := range []string{"recordings-a", "recordings-b", "recordings-c"} {
		if err := harness.recordings(directory, 1, true); err != nil {
			return err
		}
	}
	return harness.checkLogs("play-*.log", "publish-*.log")
}
