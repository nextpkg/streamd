package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/teststore"
)

type recordingGeneration struct {
	Name    string `json:"name"`
	Driver  string `json:"driver"`
	Mode    string `json:"mode"`
	Segment int    `json:"segment"`
	Swapped bool   `json:"swapped"`
	Bucket  string `json:"bucket,omitempty"`
}

var recordingTargetGenerations = []recordingGeneration{
	{Name: "fs-initial", Driver: "fs", Mode: "round", Segment: 4},
	{Name: "aws-round", Driver: "aws", Mode: "round", Segment: 8, Bucket: "test-bucket"},
	{Name: "aws-immediate", Driver: "aws", Mode: "immediate", Segment: 2, Swapped: true, Bucket: "rotated-bucket"},
	{Name: "cos-round", Driver: "cos", Mode: "round", Segment: 4},
	{Name: "fs-final", Driver: "fs", Mode: "round", Segment: 8, Swapped: true},
}

type recordingTargetCallbacks struct {
	mu           sync.Mutex
	observations []callbackObservation
	invalid      string
}

func (receiver *recordingTargetCallbacks) serve(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(io.LimitReader(request.Body, 65537))
	var payload map[string]any
	if err == nil && len(body) <= 65536 {
		err = json.Unmarshal(body, &payload)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if err != nil || len(body) > 65536 || request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" {
		receiver.invalid = "invalid recording callback envelope"
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	receiver.observations = append(receiver.observations, callbackObservation{Path: request.URL.Path, Received: time.Now().UnixMilli(), Payload: payload, Status: http.StatusOK})
	writer.WriteHeader(http.StatusOK)
}

func (receiver *recordingTargetCallbacks) snapshot() ([]callbackObservation, string) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	return append([]callbackObservation(nil), receiver.observations...), receiver.invalid
}

func validateRecordingTargetCallbacks(observations []callbackObservation, files []string, started int64) (map[string]int, error) {
	policies := map[string]recordingGeneration{}
	for _, generation := range recordingTargetGenerations {
		policies[generation.Name] = generation
	}
	pairs := map[string]map[string]float64{}
	counts := map[string]int{}
	for _, observation := range observations {
		generation, exists := policies[strings.TrimPrefix(observation.Path, "/")]
		payload := observation.Payload
		key, keyOK := payload["key"].(string)
		location, locationOK := payload["location"].(string)
		occur, timeOK := payload["occur"].(float64)
		event, eventOK := payload["event"].(string)
		connection, connectionOK := payload["connInfo"].(map[string]any)
		if !exists || !keyOK || (key != "flv/sample" && key != "ts/sample") || !locationOK || !strings.HasPrefix(location, "/"+key+"/") || !timeOK || occur < float64(started) || occur > float64(observation.Received) || !eventOK || (event != "OnRecord" && event != "OnRecordDone") || !connectionOK || connection["app"] != strings.Split(key, "/")[0] || len(payload) != 6 || payload["filename"] != path.Base(location) || observation.Status != http.StatusOK {
			return nil, errors.New("recording target callback identity, time or envelope mismatch")
		}
		format := strings.Split(key, "/")[0]
		if generation.Swapped {
			if format == "flv" {
				format = "ts"
			} else {
				format = "flv"
			}
		}
		if path.Ext(location) != "."+format {
			return nil, errors.New("recording format did not follow its generation")
		}
		timestamp, err := strconv.ParseInt(strings.TrimSuffix(path.Base(location), "."+format), 10, 64)
		if err != nil || timestamp <= 0 || (generation.Mode == "round" && timestamp%int64(generation.Segment) != 0) {
			return nil, errors.New("recording name lost round alignment")
		}
		identity := generation.Name + location
		if pairs[identity] == nil {
			pairs[identity] = map[string]float64{}
		}
		if pairs[identity][event] != 0 {
			return nil, fmt.Errorf("duplicate %s for %s", event, identity)
		}
		pairs[identity][event] = occur
		counts[generation.Name+"/"+key+"/"+event]++
	}
	if len(pairs) != len(files) {
		return nil, errors.New("callback/file cardinality mismatch")
	}
	seen := map[string]bool{}
	for _, filename := range files {
		pair := pairs[filename]
		if seen[filename] || len(pair) != 2 || pair["OnRecordDone"] < pair["OnRecord"] {
			return nil, fmt.Errorf("unmatched recording file or completion: %s", filename)
		}
		seen[filename] = true
	}
	for _, generation := range recordingTargetGenerations {
		for _, key := range []string{"flv/sample", "ts/sample"} {
			if counts[generation.Name+"/"+key+"/OnRecord"] < 1 {
				return nil, fmt.Errorf("missing recording generation %s for %s", generation.Name, key)
			}
		}
	}
	return counts, nil
}

func (harness *harness) recordingTargets() (result error) {
	container, err := harness.startObjectContainer()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, container.close(harness)) }()
	cosStore := teststore.NewHTTP("")
	defer cosStore.Close()
	httpClient := &http.Client{Timeout: 15 * time.Second}
	defer httpClient.CloseIdleConnections()
	reader := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: &container.endpoint, UsePathStyle: true, HTTPClient: httpClient, Credentials: credentials.NewStaticCredentialsProvider("test-id", "test-secret", ""), RetryMaxAttempts: 1})
	bucket := "test-bucket"
	if err := harness.until("recording target bucket ready", 30*time.Second, func() (bool, error) {
		_, err := reader.ListObjectsV2(harness.ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
		return err == nil, nil
	}); err != nil {
		return err
	}
	if _, err := reader.CreateBucket(harness.ctx, &s3.CreateBucketInput{Bucket: aws.String("rotated-bucket")}); err != nil {
		return err
	}
	receiver := &recordingTargetCallbacks{}
	endpoint := httptest.NewServer(http.HandlerFunc(receiver.serve))
	defer endpoint.Close()
	defer func() {
		observations, invalid := receiver.snapshot()
		harness.results["recording_target_callbacks"], harness.results["callback_invalid"] = observations, invalid
	}()
	harness.recordingApplications("round", 4, 2)
	configure := func(generation recordingGeneration) {
		for index := range harness.settings.Application {
			app := &harness.settings.Application[index]
			format := app.AppName
			if generation.Swapped {
				if format == "flv" {
					format = "ts"
				} else {
					format = "flv"
				}
			}
			app.Recorder = recorderConfig{Enable: true, SaveAs: format, SaveMode: generation.Mode, Segment: generation.Segment, Driver: generation.Driver, SaveTo: filepath.Join(harness.evidence, "recordings", generation.Name), Event: &config.RecorderEvent{OnRecord: endpoint.URL + "/" + generation.Name, OnRecordDone: endpoint.URL + "/" + generation.Name}}
			if generation.Driver == "aws" {
				app.Recorder.AWS = &objectConfig{Enable: true, Endpoint: container.endpoint, Bucket: generation.Bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: true, Path: generation.Name}
			}
			if generation.Driver == "cos" {
				app.Recorder.COS = &objectConfig{Enable: true, Endpoint: cosStore.URL, SecretID: "test-id", SecretKey: "test-secret", Path: generation.Name}
			}
		}
	}
	configure(recordingTargetGenerations[0])
	harness.results["recording_target_generations"] = recordingTargetGenerations
	harness.results["recording_target_scope"] = "filesystem plus two buckets in a real S3-compatible container plus public COS SDK against local HTTP contract server; no vendor credential certification"
	if err := harness.start(); err != nil {
		return err
	}
	defer harness.cleanup()
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	players, err := harness.playersForApps(50)
	if err != nil {
		return err
	}
	watch := harness.watchPlayback(players)
	defer func() { result = errors.Join(result, watch.finish(harness)) }()
	active := map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}
	for index, second := range []int{12, 24, 36} {
		if err := harness.waitAt(second); err != nil {
			return err
		}
		configure(recordingTargetGenerations[index+1])
		if err := harness.commit(recordingTargetGenerations[index+1].Name, active); err != nil {
			return err
		}
	}
	if err := harness.waitAt(46); err != nil {
		return err
	}
	for index := range harness.settings.Application {
		harness.settings.Application[index].Recorder.Enable = false
	}
	if err := harness.commit("disable-cloud-recording", map[componentKey]string{{"recording", "flv/sample"}: "disabled", {"recording", "ts/sample"}: "disabled"}); err != nil {
		return err
	}
	if err := harness.until("retired cloud generations complete", 4*time.Second, func() (bool, error) {
		if cosStore.PendingUploads() != 0 {
			return false, nil
		}
		for _, bucket := range []string{"test-bucket", "rotated-bucket"} {
			page, err := reader.ListMultipartUploads(harness.ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket})
			if err != nil {
				return false, err
			}
			if len(page.Uploads) != 0 || aws.ToBool(page.IsTruncated) {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	before, err := harness.recordingTargetInventory(reader, cosStore, false)
	if err != nil {
		return err
	}
	if err := harness.sleep(time.Second); err != nil {
		return err
	}
	after, err := harness.recordingTargetInventory(reader, cosStore, false)
	if err != nil {
		return err
	}
	if len(before) == 0 || !maps.Equal(before, after) {
		return errors.New("disabled target recordings changed")
	}
	harness.results["disabled_target_recordings_stopped"] = true
	if err := harness.waitAt(52); err != nil {
		return err
	}
	configure(recordingTargetGenerations[4])
	if err := harness.commit("reenable-filesystem-round-swapped-formats", active); err != nil {
		return err
	}
	if err := harness.finishPlayers(players, 1125); err != nil {
		return err
	}
	if err := harness.finishPublishers(publishers, "publish-", 70*time.Second); err != nil {
		return err
	}
	if err := harness.until("natural recording completion callbacks", 5*time.Second, func() (bool, error) {
		inventory, err := harness.recordingTargetInventory(reader, cosStore, false)
		if err != nil {
			return false, err
		}
		files := make([]string, 0, len(inventory))
		for name := range inventory {
			files = append(files, name)
		}
		observations, invalid := receiver.snapshot()
		if invalid != "" {
			return false, errors.New(invalid)
		}
		_, err = validateRecordingTargetCallbacks(observations, files, harness.started.UnixMilli())
		return err == nil, nil
	}); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	inventory, err := harness.recordingTargetInventory(reader, cosStore, true)
	if err != nil {
		return err
	}
	for key, size := range after {
		if inventory[key] != size {
			return fmt.Errorf("retired target changed after re-enabling recording: %s", key)
		}
	}
	harness.results["retired_target_sizes_preserved"] = true
	harness.results["object_cleanup"] = map[string]any{"pending_uploads": 0, "remaining_checkpoints": 0}
	files := make([]string, 0, len(inventory))
	for name := range inventory {
		files = append(files, name)
	}
	observations, invalid := receiver.snapshot()
	if invalid != "" {
		return errors.New(invalid)
	}
	counts, err := validateRecordingTargetCallbacks(observations, files, harness.started.UnixMilli())
	if err != nil {
		return err
	}
	harness.results["recording_target_callback_counts"], harness.results["recording_target_inventory"] = counts, inventory
	for _, generation := range recordingTargetGenerations {
		if err := harness.recordingsWithDuration(filepath.Join("recordings", generation.Name), 2, true, float64(generation.Segment)+2.5); err != nil {
			return err
		}
	}
	if err := harness.cloudServerErrors(); err != nil {
		return err
	}
	if err := harness.checkLogs("publish-*.log", "play-*.log"); err != nil {
		return err
	}
	harness.results["recording_target_transitions_verified"] = true
	return nil
}

func (harness *harness) recordingTargetInventory(reader *s3.Client, cosStore *teststore.Server, download bool) (map[string]int64, error) {
	ctx, cancel := context.WithTimeout(harness.ctx, 15*time.Second)
	defer cancel()
	inventory := map[string]int64{}
	save := func(key string, body io.ReadCloser, size int64) error {
		defer body.Close()
		if !filepath.IsLocal(key) || (path.Ext(key) != ".flv" && path.Ext(key) != ".ts") {
			return fmt.Errorf("unexpected recording object %s", key)
		}
		if download {
			filename := filepath.Join(harness.evidence, "recordings", filepath.FromSlash(key))
			if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
				return err
			}
			file, err := os.Create(filename)
			if err != nil {
				return err
			}
			count, copyErr := io.Copy(file, body)
			if err := errors.Join(copyErr, file.Close()); err != nil {
				return err
			}
			if size >= 0 && count != size {
				return errors.New("downloaded recording length mismatch")
			}
			size = count
		} else {
			count, err := io.Copy(io.Discard, body)
			if err != nil {
				return err
			}
			if size >= 0 && count != size {
				return errors.New("recording snapshot length mismatch")
			}
			size = count
		}
		if _, exists := inventory[key]; exists {
			return errors.New("recording target key collision")
		}
		inventory[key] = size
		return nil
	}
	for _, generation := range recordingTargetGenerations {
		if generation.Driver != "aws" {
			continue
		}
		uploads, err := reader.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &generation.Bucket})
		if err != nil {
			return nil, err
		}
		if len(uploads.Uploads) != 0 || aws.ToBool(uploads.IsTruncated) {
			return nil, errors.New("retired S3 target has pending uploads")
		}
		objects := s3.NewListObjectsV2Paginator(reader, &s3.ListObjectsV2Input{Bucket: &generation.Bucket})
		for objects.HasMorePages() {
			page, err := objects.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, object := range page.Contents {
				key := aws.ToString(object.Key)
				if !strings.HasPrefix(key, generation.Name+"/") {
					return nil, errors.New("S3 recording escaped configured prefix or left checkpoint")
				}
				response, err := reader.GetObject(ctx, &s3.GetObjectInput{Bucket: &generation.Bucket, Key: &key})
				if err != nil {
					return nil, err
				}
				if err := save(key, response.Body, aws.ToInt64(object.Size)); err != nil {
					return nil, err
				}
			}
		}
	}
	if cosStore.PendingUploads() != 0 || len(cosStore.Errors()) != 0 {
		return nil, errors.New("COS upload or SDK contract residue")
	}
	for _, key := range cosStore.ObjectKeys() {
		if !strings.HasPrefix(key, "cos-round/") {
			return nil, errors.New("COS recording escaped configured prefix or left checkpoint")
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, cosStore.URL+"/"+key, nil)
		if err != nil {
			return nil, err
		}
		response, err := harness.httpClient.Do(request)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			return nil, errors.New("COS recording download failed")
		}
		if err := save(key, response.Body, response.ContentLength); err != nil {
			return nil, err
		}
	}
	for _, name := range []string{"fs-initial", "fs-final"} {
		directory := filepath.Join(harness.evidence, "recordings", name)
		files, err := recordingFiles(directory)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, filename := range files {
			relative, err := filepath.Rel(filepath.Join(harness.evidence, "recordings"), filename)
			if err != nil {
				return nil, err
			}
			info, err := os.Stat(filename)
			if err != nil {
				return nil, err
			}
			inventory[filepath.ToSlash(relative)] = info.Size()
		}
	}
	return inventory, nil
}
