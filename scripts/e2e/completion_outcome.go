package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nextpkg/streamd/internal/mediatime"
	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
)

func (harness *harness) completionOutcome() (result error) {
	provider, mode := os.Getenv("STREAMD_E2E_OUTCOME_PROVIDER"), os.Getenv("STREAMD_E2E_OUTCOME_FAULT")
	if provider == "" {
		provider = "aws"
	}
	if mode == "" {
		mode = "response-lost"
	}
	if provider != "aws" && provider != "cos" {
		return errors.New("STREAMD_E2E_OUTCOME_PROVIDER must be aws or cos")
	}
	if remaining := 86400 - time.Now().Unix()%86400; remaining < 120 {
		return errors.New("completion outcome fixture needs 120 seconds before its day bucket expires")
	}
	var contract *teststore.Server
	var reader *s3.Client
	var endpoint, bucket string
	if provider == "aws" {
		container, err := harness.startObjectContainer()
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, container.close(harness)) }()
		endpoint, bucket = container.endpoint, "test-bucket"
		reader = s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: &endpoint, UsePathStyle: true, HTTPClient: harness.httpClient, Credentials: credentials.NewStaticCredentialsProvider("test-id", "test-secret", ""), RetryMaxAttempts: 1})
		if err := harness.until("outcome bucket ready", 30*time.Second, func() (bool, error) {
			_, err := reader.ListObjectsV2(harness.ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
			return err == nil, err
		}); err != nil {
			return err
		}
	} else {
		contract = teststore.NewHTTP("")
		defer contract.Close()
		endpoint = contract.URL
	}
	fault, err := teststore.NewCompletionFault(endpoint, bucket, mode)
	if err != nil {
		return err
	}
	defer fault.Close()
	defer func() {
		outcomes, failures := fault.Snapshot()
		harness.results["completion_outcomes"], harness.results["completion_proxy_failures"] = outcomes, failures
	}()
	configuration := &objectConfig{Enable: true, Endpoint: fault.URL, Bucket: bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: provider == "aws"}
	var probe *objectstore.Client
	if provider == "aws" {
		probe, err = objectstore.NewS3(&objectstore.S3Config{Enable: true, Endpoint: fault.URL, Bucket: bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: true})
	} else {
		probe, err = objectstore.NewCOS(&objectstore.COSConfig{Enable: true, Endpoint: fault.URL, SecretID: "test-id", SecretKey: "test-secret"})
	}
	if err != nil {
		return err
	}
	defer probe.Close()
	harness.results["completion_provider"], harness.results["completion_fault"] = provider, mode
	harness.results["completion_scope"] = "upstream successfully completes actual media before response loss or checkpoint delete denial; retained ambiguity is not automatic reconciliation; COS uses a local SDK contract and AWS a real compatible container"
	harness.recordingApplications("round", 86400, 2)
	for index := range harness.settings.Application {
		recorder := &harness.settings.Application[index].Recorder
		recorder.Driver = provider
		if provider == "aws" {
			recorder.AWS = configuration
		} else {
			recorder.COS = configuration
		}
	}
	callbacks := harness.storageCallbacks()
	defer callbacks.server.Close()
	defer func() {
		observations, invalid := callbacks.receiver.snapshot()
		harness.results["completion_callbacks"], harness.results["completion_callback_invalid"] = observations, invalid
	}()
	if err := harness.start(); err != nil {
		return err
	}
	defer harness.cleanup()
	seed, err := harness.publishApps("seed-", "bframes", 5)
	if err != nil {
		return err
	}
	for app, publisher := range seed {
		if err := harness.finish("seed-"+app, publisher, 15*time.Second); err != nil {
			return err
		}
	}
	if err := harness.until("paused cloud recorders retired", 5*time.Second, func() (bool, error) {
		report, err := harness.configuration()
		for _, component := range report.Components {
			if component.Component == "recording" {
				return false, err
			}
		}
		return err == nil, err
	}); err != nil {
		return err
	}
	saved, err := harness.completionObjects(reader, contract)
	if err != nil || len(saved) != 2 {
		return fmt.Errorf("expected two durable pause checkpoints: count=%d error=%v", len(saved), err)
	}
	keys := []string{}
	for filename, body := range saved {
		var checkpoint struct {
			Key   string           `json:"key"`
			ID    string           `json:"upload_id"`
			Media *mediatime.State `json:"media"`
		}
		if !strings.Contains(filename, "/.streamd-checkpoints/") || json.Unmarshal(body, &checkpoint) != nil || checkpoint.ID == "" || checkpoint.Media == nil || checkpoint.Media.Bytes == 0 || checkpoint.Media.Next == 0 {
			return errors.New("seed did not produce a resumable media checkpoint")
		}
		keys = append(keys, checkpoint.Key)
	}
	sort.Strings(keys)
	uploads, err := probe.Backend.Uploads(harness.ctx, "")
	if err != nil || len(uploads) != 2 {
		return fmt.Errorf("seed has %d pending uploads: %v", len(uploads), err)
	}
	harness.results["completion_seed_uploads"] = uploads
	harness.results["completion_seed_checkpoints"] = byteDigests(saved)
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	if err := harness.until("resumed cloud media active", 5*time.Second, func() (bool, error) {
		report, err := harness.configuration()
		return componentsApplied(report, report.Revision, map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}), err
	}); err != nil {
		return err
	}
	players, err := harness.playersForApps(50)
	if err != nil {
		return err
	}
	watch := harness.watchPlayback(players)
	defer func() { result = errors.Join(result, watch.finish(harness)) }()
	if err := harness.waitAt(20); err != nil {
		return err
	}
	if err := fault.Arm(keys); err != nil {
		return err
	}
	for index := range harness.settings.Application {
		harness.settings.Application[index].Recorder.Enable = false
	}
	if err := harness.commit("disable-resumed-cloud-recording", map[componentKey]string{{"recording", "flv/sample"}: "disabled", {"recording", "ts/sample"}: "disabled"}); err != nil {
		return err
	}
	for index := range harness.settings.Application {
		app := &harness.settings.Application[index]
		app.Recorder = recorderConfig{Enable: true, Driver: "fs", SaveAs: app.AppName, SaveMode: "immediate", Segment: 4, SaveTo: filepath.Join(harness.evidence, "recovered"), Event: callbacks.recordingEvent("recovered")}
	}
	if err := harness.commit("recover-both-recorders-to-filesystem", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
		return err
	}
	if err := harness.until("ambiguous completion remains visible after recovery", 8*time.Second, func() (bool, error) {
		report, err := harness.configuration()
		if err != nil {
			return false, err
		}
		count := 0
		for _, state := range report.Components {
			if state.Component == "recording" && state.State == "active" && state.Revision == report.Revision && state.Failures == 1 && state.LastFailureAt > 0 {
				count++
			}
		}
		harness.results["completion_recovered_status"] = report
		return count == 2, nil
	}); err != nil {
		return err
	}
	completed, err := harness.completionObjects(reader, contract)
	if err != nil {
		return err
	}
	for key, body := range completed {
		directory := "retained-checkpoints"
		if path.Ext(key) == ".flv" || path.Ext(key) == ".ts" {
			directory = "cloud-recordings"
		}
		filename := filepath.Join(harness.evidence, directory, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
			return err
		}
		if err := os.WriteFile(filename, body, 0600); err != nil {
			return err
		}
	}
	outcomes, failures := fault.Snapshot()
	if err := validateCompletionObjects(saved, completed, outcomes, mode); err != nil || len(failures) != 0 {
		return fmt.Errorf("completion outcome evidence: %v proxy=%v", err, failures)
	}
	for _, key := range keys {
		for attempt := 0; attempt < 2; attempt++ {
			_, err := probe.NewMultipartUpload(harness.ctx, key, true)
			if err == nil || !strings.Contains(err.Error(), "missing multipart upload") {
				return fmt.Errorf("orphan checkpoint allowed unsafe reopen: %v", err)
			}
		}
	}
	if err := harness.finishPlayers(players, 1125); err != nil {
		return err
	}
	for app, publisher := range publishers {
		if err := harness.finish("publish-"+app, publisher, 75*time.Second); err != nil {
			return err
		}
	}
	files, err := recordingFiles(filepath.Join(harness.evidence, "recovered"))
	if err != nil {
		return err
	}
	relative := []string{}
	for _, filename := range files {
		name, err := filepath.Rel(filepath.Join(harness.evidence, "recovered"), filename)
		if err != nil {
			return err
		}
		relative = append(relative, filepath.ToSlash(name))
	}
	if err := harness.until("all completion outcome callbacks", 5*time.Second, func() (bool, error) {
		observations, invalid := callbacks.receiver.snapshot()
		if invalid != "" {
			return false, errors.New(invalid)
		}
		counts, err := validateCompletionCallbacks(observations, relative, keys, harness.started.UnixMilli())
		harness.results["completion_callback_counts"] = counts
		return err == nil, err
	}); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	after, err := harness.completionObjects(reader, contract)
	if err != nil || !maps.Equal(byteDigests(completed), byteDigests(after)) {
		return fmt.Errorf("completed cloud evidence changed after rejected opens/recovery: %v", err)
	}
	uploads, err = probe.Backend.Uploads(harness.ctx, "")
	if err != nil || len(uploads) != 0 {
		return fmt.Errorf("committed cloud upload still pending: %v %v", uploads, err)
	}
	outcomes, failures = fault.Snapshot()
	if err := validateCompletionObjects(saved, after, outcomes, mode); err != nil || len(failures) != 0 {
		return fmt.Errorf("final completion evidence changed: %v proxy=%v", err, failures)
	}
	harness.results["completion_final_objects"] = byteDigests(after)
	harness.results["completion_retained_checkpoints"] = 2
	harness.results["completion_pending_uploads"] = 0
	if err := harness.recordingsWithDuration("cloud-recordings", 2, true, 30); err != nil {
		return err
	}
	extents := map[string]any{}
	for _, key := range keys {
		probe, err := harness.probePackets(filepath.Join(harness.evidence, "cloud-recordings", filepath.FromSlash(key)))
		if err != nil {
			return err
		}
		duration, err := strconv.ParseFloat(probe.Format.Duration, 64)
		if err != nil || duration < 12 || duration > 30 {
			return errors.New("completed recording does not contain enough resumed media")
		}
		frames := map[string]int{}
		for _, stream := range probe.Streams {
			frames[stream.CodecType], err = strconv.Atoi(stream.Frames)
			if err != nil {
				return err
			}
		}
		if frames["video"] < 300 || frames["audio"] < 400 {
			return errors.New("completed recording only covers the seed or lacks resumed frames")
		}
		previous := map[string]float64{}
		for _, packet := range probe.Packets {
			timestamp, err := strconv.ParseFloat(packet.DTS, 64)
			if err != nil || math.IsNaN(timestamp) || math.IsInf(timestamp, 0) {
				return errors.New("invalid completed media DTS")
			}
			if last, exists := previous[packet.CodecType]; exists && (timestamp <= last || timestamp-last > 0.25) {
				return fmt.Errorf("completed media DTS discontinuity: %s %.6f -> %.6f", key, last, timestamp)
			}
			previous[packet.CodecType] = timestamp
		}
		extents[key] = map[string]any{"duration": duration, "decoded_frames": frames, "raw_dts_continuous": true}
	}
	harness.results["completion_media_extents"] = extents
	if err := harness.recordings("recovered", 16, true); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	expected, err := completionOutcomeErrors(body, mode)
	harness.results["expected_server_errors"] = expected
	if err != nil {
		return err
	}
	if err := harness.checkLogs("publish-*.log", "seed-*.log", "play-*.log"); err != nil {
		return err
	}
	harness.results["completion_outcome_verified"] = true
	return nil
}

func byteDigests(objects map[string][]byte) map[string]string {
	result := map[string]string{}
	for key, data := range objects {
		result[key] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	return result
}

func validateCompletionObjects(saved, completed map[string][]byte, outcomes map[string]teststore.CompletionOutcome, mode string) error {
	if mode != "response-lost" && mode != "cleanup-denied" {
		return errors.New("unknown completion evidence mode")
	}
	if len(saved) != 2 || len(completed) != 4 || len(outcomes) != 2 {
		return errors.New("incomplete checkpoint/object/upload inventory")
	}
	for key, before := range saved {
		if !bytes.Equal(before, completed[key]) {
			return errors.New("ambiguous completion changed its saved checkpoint")
		}
	}
	for _, app := range []string{"flv", "ts"} {
		matched := 0
		for key, outcome := range outcomes {
			if path.Dir(key) != app+"/sample" || path.Ext(key) != "."+app {
				continue
			}
			matched++
			if outcome.Created != 1 || outcome.SuccessfulCompletions < 1 || outcome.SuccessfulCompletions > 2 || len(outcome.CompletionManifestSHA256) != 64 || outcome.Aborted != 0 || outcome.UploadID == "" || (mode == "response-lost" && (outcome.Dropped != outcome.SuccessfulCompletions || outcome.Denied != 0)) || (mode == "cleanup-denied" && (outcome.SuccessfulCompletions != 1 || outcome.Dropped != 0 || outcome.Denied != 1)) {
				return errors.New("fault did not follow a proven completion of the same upload and manifest")
			}
			if err := teststore.VerifyCompletedParts(completed[key], outcome.Parts); err != nil {
				return err
			}
		}
		if matched != 1 {
			return errors.New("missing exact FLV/TS cloud object identity")
		}
	}
	return nil
}

func (harness *harness) completionObjects(reader *s3.Client, contract *teststore.Server) (map[string][]byte, error) {
	ctx, cancel := context.WithTimeout(harness.ctx, 10*time.Second)
	defer cancel()
	keys := []string{}
	bucket := "test-bucket"
	if reader != nil {
		page, err := reader.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
		if err != nil {
			return nil, err
		}
		if aws.ToBool(page.IsTruncated) || len(page.Contents) > 4 {
			return nil, errors.New("unexpected cloud outcome object inventory")
		}
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
	} else {
		keys = contract.ObjectKeys()
		if len(contract.Errors()) != 0 || len(keys) > 4 {
			return nil, errors.New("COS outcome contract or object inventory failed")
		}
	}
	objects := map[string][]byte{}
	for _, key := range keys {
		if !filepath.IsLocal(key) {
			return nil, errors.New("unsafe cloud evidence key")
		}
		var body io.ReadCloser
		if reader != nil {
			response, err := reader.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
			if err != nil {
				return nil, err
			}
			body = response.Body
		} else {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, contract.URL+"/"+key, nil)
			if err != nil {
				return nil, err
			}
			response, err := harness.httpClient.Do(request)
			if err != nil {
				return nil, err
			}
			if response.StatusCode != http.StatusOK {
				response.Body.Close()
				return nil, errors.New("COS outcome object unavailable")
			}
			body = response.Body
		}
		data, readErr := io.ReadAll(io.LimitReader(body, 16*1024*1024+1))
		if err := errors.Join(readErr, body.Close()); err != nil {
			return nil, err
		}
		if len(data) == 0 || len(data) > 16*1024*1024 {
			return nil, errors.New("cloud evidence outside fixture size limits")
		}
		objects[key] = data
	}
	return objects, nil
}
