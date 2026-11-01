package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
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
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/teststore"
)

type roundExpiryState struct {
	Natural []string       `json:"natural"`
	Active  []string       `json:"active"`
	Counts  map[string]int `json:"counts"`
}

func validateRoundExpiry(observations []callbackObservation, started int64, forced []string, finished bool) (roundExpiryState, error) {
	state := roundExpiryState{Counts: map[string]int{}}
	pairs := map[string]map[string]int64{}
	forcedFiles := map[string]bool{}
	for _, filename := range forced {
		if forcedFiles[filename] {
			return state, errors.New("duplicate forced tail")
		}
		forcedFiles[filename] = true
	}
	for _, observation := range observations {
		payload := observation.Payload
		event, eventOK := payload["event"].(string)
		key, keyOK := payload["key"].(string)
		occur, timeOK := payload["occur"].(float64)
		if !eventOK || !keyOK || (key != "flv/sample" && key != "ts/sample") || !timeOK || math.IsNaN(occur) || math.IsInf(occur, 0) || occur != math.Trunc(occur) || occur < float64(started) || occur > float64(observation.Received) || observation.Status != http.StatusOK {
			return state, errors.New("invalid round callback identity, time or status")
		}
		state.Counts[key+"/"+event]++
		switch event {
		case "OnPublish", "OnPublishDone":
			if len(payload) != 3 || observation.Path != "/publication" {
				return state, errors.New("invalid publication callback")
			}
		case "OnRecord", "OnRecordDone":
			location, locationOK := payload["location"].(string)
			connection, connectionOK := payload["connInfo"].(map[string]any)
			app := strings.Split(key, "/")[0]
			if !locationOK || !connectionOK || connection["app"] != app || observation.Path != "/round" || len(payload) != 6 || path.Dir(location) != "/"+key || path.Ext(location) != "."+app || payload["filename"] != path.Base(location) {
				return state, errors.New("invalid round recording envelope")
			}
			bucket, err := strconv.ParseInt(strings.TrimSuffix(path.Base(location), "."+app), 10, 64)
			if err != nil || bucket <= 0 || bucket%8 != 0 || bucket > math.MaxInt64/1000-8 {
				return state, errors.New("invalid round bucket alignment")
			}
			if event == "OnRecord" && (occur < float64(bucket*1000) || occur >= float64((bucket+8)*1000)) {
				return state, errors.New("recording opened outside its time bucket")
			}
			if event == "OnRecordDone" && !forcedFiles[location] && occur < float64((bucket+8)*1000) {
				return state, errors.New("natural completion preceded round expiry")
			}
			if pairs[location] == nil {
				pairs[location] = map[string]int64{}
			}
			if pairs[location][event] != 0 {
				return state, errors.New("duplicate round callback")
			}
			pairs[location][event] = int64(occur)
		default:
			return state, errors.New("unknown round callback")
		}
	}
	for filename, pair := range pairs {
		if pair["OnRecord"] == 0 || (pair["OnRecordDone"] != 0 && pair["OnRecordDone"] < pair["OnRecord"]) {
			return state, errors.New("missing start or reversed completion")
		}
		if pair["OnRecordDone"] == 0 {
			if finished {
				return state, errors.New("recording never completed")
			}
			state.Active = append(state.Active, filename)
		} else if !forcedFiles[filename] {
			state.Natural = append(state.Natural, filename)
		}
	}
	for _, app := range []string{"flv", "ts"} {
		key := app + "/sample/"
		expectedDone := 0
		if finished {
			expectedDone = 1
		}
		if state.Counts[key+"OnPublish"] != 1 || state.Counts[key+"OnPublishDone"] != expectedDone {
			return state, errors.New("publication lifecycle count mismatch")
		}
		natural, active, tails := 0, 0, 0
		for _, filename := range state.Natural {
			if strings.HasPrefix(filename, "/"+key) {
				natural++
			}
		}
		for _, filename := range state.Active {
			if strings.HasPrefix(filename, "/"+key) {
				active++
			}
		}
		for filename := range forcedFiles {
			if pairs[filename]["OnRecordDone"] == 0 {
				return state, errors.New("unknown or unfinished forced tail")
			}
			if strings.HasPrefix(filename, "/"+key) {
				tails++
			}
		}
		if natural < 4 || (!finished && (active != 1 || tails != 0)) || (finished && (active != 0 || tails != 1)) {
			return state, errors.New("insufficient natural boundaries or incorrect active/forced tails")
		}
	}
	sort.Strings(state.Natural)
	sort.Strings(state.Active)
	return state, nil
}

func (harness *harness) roundExpiry() (result error) {
	backend := os.Getenv("STREAMD_E2E_ROUND_BACKEND")
	if backend == "" {
		backend = "fs"
	}
	if backend != "fs" && backend != "aws" && backend != "cos" {
		return errors.New("STREAMD_E2E_ROUND_BACKEND must be fs, aws or cos")
	}
	var reader *s3.Client
	var contract *teststore.Server
	var object *objectConfig
	bucket := "test-bucket"
	harness.results["round_backend"] = backend
	harness.results["round_scope"] = "foreground wall-clock expiry during continuous publishing; final active tails separately completed by disabling recording"
	switch backend {
	case "aws":
		container, err := harness.startObjectContainer()
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, container.close(harness)) }()
		object = &objectConfig{Enable: true, Endpoint: container.endpoint, Bucket: bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: true}
		reader = s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: &container.endpoint, UsePathStyle: true, HTTPClient: harness.httpClient, Credentials: credentials.NewStaticCredentialsProvider(object.SecretID, object.SecretKey, ""), RetryMaxAttempts: 1})
		if err := harness.until("round bucket ready", 30*time.Second, func() (bool, error) {
			_, err := reader.ListObjectsV2(harness.ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
			return err == nil, err
		}); err != nil {
			return err
		}
		harness.results["round_backend_scope"] = "actual pinned SeaweedFS S3-compatible container; not AWS vendor certification"
	case "cos":
		contract = teststore.NewHTTP("")
		defer contract.Close()
		object = &objectConfig{Enable: true, Endpoint: contract.URL, SecretID: "test-id", SecretKey: "test-secret"}
		harness.results["round_backend_scope"] = "public COS SDK against local HTTP contract server; not COS vendor certification"
	}
	receiver := &recordingTargetCallbacks{}
	callbacks := httptest.NewServer(http.HandlerFunc(receiver.serve))
	defer callbacks.Close()
	defer func() {
		observations, invalid := receiver.snapshot()
		harness.results["round_callbacks"], harness.results["round_callback_invalid"] = observations, invalid
	}()
	harness.recordingApplications("round", 8, 2)
	for index := range harness.settings.Application {
		app := &harness.settings.Application[index]
		app.Event = &config.AppEvent{OnPublish: callbacks.URL + "/publication", OnPublishDone: callbacks.URL + "/publication"}
		app.Recorder.Driver = backend
		app.Recorder.Event = &config.RecorderEvent{OnRecord: callbacks.URL + "/round", OnRecordDone: callbacks.URL + "/round"}
		switch backend {
		case "aws":
			app.Recorder.AWS = object
		case "cos":
			app.Recorder.COS = object
		}
	}
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
	if err := harness.waitAt(45); err != nil {
		return err
	}
	if err := harness.until("stable interior of round bucket", 9*time.Second, func() (bool, error) {
		return time.Now().UnixMilli()%8000 >= 3000 && time.Now().UnixMilli()%8000 < 3500, nil
	}); err != nil {
		return err
	}
	observations, invalid := receiver.snapshot()
	if invalid != "" {
		return errors.New(invalid)
	}
	live, err := validateRoundExpiry(observations, harness.started.UnixMilli(), nil, false)
	if err != nil {
		return err
	}
	harness.results["round_before_disable"] = live
	harness.results["round_before_disable_callbacks"] = observations
	before, err := harness.roundExpiryInventory(reader, contract, live.Active, false)
	if err != nil {
		return err
	}
	if !sameRoundFiles(before, live.Natural) {
		return errors.New("naturally completed callbacks do not match independently listed media")
	}
	for _, publisher := range publishers {
		if !publisher.alive() {
			return errors.New("publisher ended before natural expiry verification")
		}
	}
	harness.results["round_natural_sha256"] = before
	harness.results["round_disable_requested_milliseconds"] = time.Now().UnixMilli()
	for index := range harness.settings.Application {
		harness.settings.Application[index].Recorder.Enable = false
	}
	if err := harness.commit("disable-round-after-natural-expiry", map[componentKey]string{{"recording", "flv/sample"}: "disabled", {"recording", "ts/sample"}: "disabled"}); err != nil {
		return err
	}
	if err := harness.finishPlayers(players, 1125); err != nil {
		return err
	}
	if err := harness.finishPublishers(publishers, "publish-", 70*time.Second); err != nil {
		return err
	}
	var final roundExpiryState
	if err := harness.until("round final callback pairs", 5*time.Second, func() (bool, error) {
		observations, invalid := receiver.snapshot()
		if invalid != "" {
			return false, errors.New(invalid)
		}
		var err error
		final, err = validateRoundExpiry(observations, harness.started.UnixMilli(), live.Active, true)
		return err == nil, err
	}); err != nil {
		return err
	}
	after, err := harness.roundExpiryInventory(reader, contract, nil, true)
	if err != nil {
		return err
	}
	if !sameRoundFiles(after, append(append([]string{}, final.Natural...), live.Active...)) || len(after) != len(before)+2 {
		return errors.New("final recordings differ from natural files and two explicit tails")
	}
	for filename, digest := range before {
		if after[filename] != digest {
			return errors.New("completed round object changed after recording disabled")
		}
	}
	harness.results["round_final"] = final
	harness.results["round_final_sha256"] = after
	harness.results["object_cleanup"] = map[string]int{"pending_uploads": 0, "remaining_checkpoints": 0}
	if err := harness.stopServer(); err != nil {
		return err
	}
	if err := harness.recordingsWithDuration("recordings", 10, true, 10.5); err != nil {
		return err
	}
	if err := harness.cloudServerErrors(); err != nil {
		return err
	}
	if err := harness.checkLogs("publish-*.log", "play-*.log"); err != nil {
		return err
	}
	harness.results["round_expiry_verified"] = true
	return nil
}

func sameRoundFiles(inventory map[string]string, files []string) bool {
	if len(inventory) != len(files) {
		return false
	}
	seen := map[string]bool{}
	for _, filename := range files {
		if inventory[filename] == "" || seen[filename] {
			return false
		}
		seen[filename] = true
	}
	return true
}

func (harness *harness) roundExpiryInventory(reader *s3.Client, contract *teststore.Server, active []string, download bool) (map[string]string, error) {
	keys := []string{}
	bucket := "test-bucket"
	if reader != nil {
		pending, err := reader.ListMultipartUploads(harness.ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket})
		if err != nil {
			return nil, err
		}
		if len(pending.Uploads) != len(active) || aws.ToBool(pending.IsTruncated) {
			return nil, errors.New("S3 pending uploads do not match active tails")
		}
		for _, upload := range pending.Uploads {
			matched := false
			for _, filename := range active {
				matched = matched || "/"+aws.ToString(upload.Key) == filename
			}
			if !matched {
				return nil, errors.New("S3 pending upload has unexpected identity")
			}
		}
		pages := s3.NewListObjectsV2Paginator(reader, &s3.ListObjectsV2Input{Bucket: &bucket})
		for pages.HasMorePages() {
			page, err := pages.NextPage(harness.ctx)
			if err != nil {
				return nil, err
			}
			for _, object := range page.Contents {
				keys = append(keys, aws.ToString(object.Key))
			}
		}
	} else if contract != nil {
		if contract.PendingUploads() != len(active) || len(contract.Errors()) != 0 {
			return nil, errors.New("COS contract errors or unexpected pending uploads")
		}
		keys = contract.ObjectKeys()
	} else {
		files, err := recordingFiles(filepath.Join(harness.evidence, "recordings"))
		if err != nil {
			return nil, err
		}
		for _, filename := range files {
			relative, err := filepath.Rel(filepath.Join(harness.evidence, "recordings"), filename)
			if err != nil {
				return nil, err
			}
			keys = append(keys, filepath.ToSlash(relative))
		}
	}
	inventory := map[string]string{}
	for _, key := range keys {
		if !filepath.IsLocal(key) || strings.Contains(key, "\\") {
			return nil, errors.New("recording key escaped test directory")
		}
		if path.Ext(key) != ".flv" && path.Ext(key) != ".ts" {
			if download {
				return nil, fmt.Errorf("checkpoint or unknown object remains: %s", key)
			}
			continue
		}
		isActive := false
		for _, filename := range active {
			isActive = isActive || filename == "/"+key
		}
		if isActive {
			if reader != nil || contract != nil {
				return nil, errors.New("active cloud tail already appears as completed object")
			}
			continue
		}
		filename := filepath.Join(harness.evidence, "recordings", filepath.FromSlash(key))
		var body io.ReadCloser
		if reader != nil {
			response, err := reader.GetObject(harness.ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
			if err != nil {
				return nil, err
			}
			body = response.Body
		} else if contract != nil {
			request, err := http.NewRequestWithContext(harness.ctx, http.MethodGet, contract.URL+"/"+key, nil)
			if err != nil {
				return nil, err
			}
			response, err := harness.httpClient.Do(request)
			if err != nil {
				return nil, err
			}
			if response.StatusCode != http.StatusOK {
				_ = response.Body.Close()
				return nil, errors.New("COS completed recording unavailable")
			}
			body = response.Body
		} else {
			file, err := os.Open(filename)
			if err != nil {
				return nil, err
			}
			body = file
		}
		data, readErr := io.ReadAll(io.LimitReader(body, 16*1024*1024+1))
		if err := errors.Join(readErr, body.Close()); err != nil {
			return nil, err
		}
		if len(data) == 0 || len(data) > 16*1024*1024 {
			return nil, errors.New("unexpected round recording size")
		}
		if download && (reader != nil || contract != nil) {
			if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
				return nil, err
			}
			if err := os.WriteFile(filename, data, 0600); err != nil {
				return nil, err
			}
		}
		inventory["/"+key] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	return inventory, nil
}
