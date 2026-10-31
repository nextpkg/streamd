package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type credentialOperation struct {
	Method     string `json:"method"`
	Credential string `json:"credential"`
	Status     int    `json:"status"`
}

type credentialUpload struct {
	Key        string                `json:"key"`
	Credential string                `json:"credential"`
	Created    time.Time             `json:"created"`
	Completed  time.Time             `json:"completed"`
	Operations []credentialOperation `json:"operations"`
}

type credentialTrace struct {
	mu                 sync.Mutex
	uploads            map[string]credentialUpload
	denied             []credentialOperation
	missingCheckpoints int
}

func signingCredential(request *http.Request) string {
	_, value, exists := strings.Cut(request.Header.Get("Authorization"), "Credential=")
	if !exists {
		return ""
	}
	credential, _, _ := strings.Cut(value, "/")
	return credential
}

func (trace *credentialTrace) response(response *http.Response) error {
	request := response.Request
	operation := credentialOperation{Method: request.Method, Credential: signingCredential(request), Status: response.StatusCode}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if response.StatusCode == http.StatusNotFound && request.Method == http.MethodGet && request.URL.Query().Get("uploadId") == "" && strings.HasSuffix(request.URL.Path, ".json") && (strings.HasPrefix(request.URL.Path, "/test-bucket/flv/sample/.streamd-checkpoints/") || strings.HasPrefix(request.URL.Path, "/test-bucket/ts/sample/.streamd-checkpoints/")) {
		body, err := io.ReadAll(io.LimitReader(response.Body, 65537))
		response.Body.Close()
		if err != nil {
			return err
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
		var failure struct{ Code string }
		if err := xml.Unmarshal(body, &failure); err != nil {
			return err
		}
		if failure.Code == "NoSuchKey" {
			trace.missingCheckpoints++
			return nil
		}
	}
	if response.StatusCode >= 400 {
		trace.denied = append(trace.denied, operation)
		return nil
	}
	if request.Method == http.MethodPost && request.URL.Query().Has("uploads") {
		body, err := io.ReadAll(io.LimitReader(response.Body, 65537))
		response.Body.Close()
		if err != nil {
			return err
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
		var created struct {
			ID string `xml:"UploadId"`
		}
		if err := xml.Unmarshal(body, &created); err != nil {
			return err
		}
		if created.ID == "" {
			return errors.New("empty multipart identity")
		}
		trace.uploads[created.ID] = credentialUpload{Key: strings.TrimPrefix(request.URL.Path, "/test-bucket/"), Credential: operation.Credential, Created: time.Now(), Operations: []credentialOperation{operation}}
	}
	if id := request.URL.Query().Get("uploadId"); id != "" {
		upload, exists := trace.uploads[id]
		if !exists {
			return errors.New("request used unknown upload identity")
		}
		upload.Operations = append(upload.Operations, operation)
		if request.Method == http.MethodPost {
			upload.Completed = time.Now()
		}
		trace.uploads[id] = upload
	}
	return nil
}

func validateCredentialUploads(uploads map[string]credentialUpload, committed time.Time) (map[string]int, error) {
	counts := map[string]int{}
	keys := map[string]bool{}
	for id, upload := range uploads {
		if id == "" || keys[upload.Key] || upload.Created.IsZero() || upload.Completed.Before(upload.Created) || (upload.Credential != "test-id" && upload.Credential != "test-next-id") {
			return nil, errors.New("upload identity was replaced, unfinished or unproven")
		}
		keys[upload.Key] = true
		app, _, _ := strings.Cut(upload.Key, "/")
		if app != "flv" && app != "ts" {
			return nil, errors.New("upload escaped media namespace")
		}
		counts[app+"/"+upload.Credential]++
		if upload.Credential == "test-id" && upload.Created.Before(committed) && upload.Completed.After(committed) {
			counts[app+"/old-completed-after-commit"]++
		}
		if upload.Credential == "test-next-id" && upload.Created.Before(committed) {
			return nil, errors.New("new credential used before commit")
		}
		created, written, completed := 0, 0, 0
		for index, operation := range upload.Operations {
			if operation.Credential != upload.Credential || operation.Status < 200 || operation.Status >= 300 {
				return nil, errors.New("in-flight upload changed credentials or failed")
			}
			switch operation.Method {
			case http.MethodPost:
				if index == 0 {
					created++
				} else {
					completed++
				}
			case http.MethodPut:
				written++
			case http.MethodGet:
			default:
				return nil, errors.New("unexpected upload operation")
			}
		}
		if created != 1 || written == 0 || completed != 1 {
			return nil, errors.New("upload lifecycle missing create, data or completion")
		}
	}
	for _, app := range []string{"flv", "ts"} {
		if counts[app+"/test-id"] < 1 || counts[app+"/test-next-id"] < 2 || counts[app+"/old-completed-after-commit"] < 1 {
			return nil, errors.New("credential handoff did not cover both live streams")
		}
	}
	return counts, nil
}

func (harness *harness) waitRejectedRecordingCandidate(operation string, revision int) error {
	message := "configuration reload rejected: prepare recording: object storage preflight " + operation + " failed"
	if err := harness.atomicSettings(); err != nil {
		return err
	}
	if err := harness.until("rejected recording "+operation, 8*time.Second, func() (bool, error) {
		body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
		return bytes.Contains(body, []byte(message)), err
	}); err != nil {
		return err
	}
	report, err := harness.configuration()
	if err != nil {
		return err
	}
	if !componentsApplied(report, revision, map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}) {
		return errors.New("rejected candidate changed active recording revision")
	}
	for _, component := range report.Components {
		if component.Failures != 0 {
			return errors.New("rejected candidate affected active media component")
		}
	}
	harness.results["rejected_candidate_"+operation] = report
	return nil
}

func (harness *harness) recordingCredentials() (result error) {
	container, err := harness.startObjectContainer()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, container.close(harness)) }()
	backend, err := url.Parse(container.endpoint)
	if err != nil {
		return err
	}
	trace := &credentialTrace{uploads: map[string]credentialUpload{}}
	proxy := httputil.NewSingleHostReverseProxy(backend)
	proxy.ModifyResponse = trace.response
	endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPut && strings.Contains(request.URL.Path, "/blocked/") && strings.Contains(request.URL.Path, "/.streamd-preflight/") {
			trace.mu.Lock()
			trace.denied = append(trace.denied, credentialOperation{Method: request.Method, Credential: signingCredential(request), Status: http.StatusForbidden})
			trace.mu.Unlock()
			writer.WriteHeader(http.StatusForbidden)
			fmt.Fprint(writer, "<Error><Code>AccessDenied</Code></Error>")
			return
		}
		proxy.ServeHTTP(writer, request)
	}))
	defer endpoint.Close()
	defer func() {
		trace.mu.Lock()
		defer trace.mu.Unlock()
		harness.results["credential_uploads"], harness.results["credential_denied_requests"] = maps.Clone(trace.uploads), append([]credentialOperation(nil), trace.denied...)
		harness.results["new_recording_checkpoint_misses"] = trace.missingCheckpoints
	}()
	httpClient := &http.Client{Timeout: 15 * time.Second}
	defer httpClient.CloseIdleConnections()
	reader := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: &container.endpoint, UsePathStyle: true, HTTPClient: httpClient, Credentials: credentials.NewStaticCredentialsProvider("test-id", "test-secret", ""), RetryMaxAttempts: 1})
	bucket := "test-bucket"
	if err := harness.until("credential bucket ready", 30*time.Second, func() (bool, error) {
		_, err := reader.ListObjectsV2(harness.ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
		return err == nil, nil
	}); err != nil {
		return err
	}
	harness.recordingApplications("round", 8, 2)
	for index := range harness.settings.Application {
		recorder := &harness.settings.Application[index].Recorder
		recorder.Driver = "aws"
		recorder.AWS = &objectConfig{Enable: true, Endpoint: endpoint.URL, Bucket: bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: true}
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
	if err := harness.waitAt(12); err != nil {
		return err
	}
	for index := range harness.settings.Application {
		harness.settings.Application[index].Recorder.AWS.SecretKey = "invalid-test-secret"
	}
	if err := harness.waitRejectedRecordingCandidate("listing", 1); err != nil {
		return err
	}
	if err := harness.waitAt(18); err != nil {
		return err
	}
	for index := range harness.settings.Application {
		cfg := harness.settings.Application[index].Recorder.AWS
		cfg.SecretID, cfg.SecretKey = "test-next-id", "test-next-secret"
	}
	if err := harness.commit("rotate-valid-recording-credentials", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
		return err
	}
	if err := harness.waitAt(32); err != nil {
		return err
	}
	for index := range harness.settings.Application {
		harness.settings.Application[index].Recorder.AWS.Path = "blocked"
	}
	if err := harness.waitRejectedRecordingCandidate("write", 2); err != nil {
		return err
	}
	for index := range harness.settings.Application {
		harness.settings.Application[index].Recorder.AWS.Path = ""
	}
	if err := harness.atomicSettings(); err != nil {
		return err
	}
	if err := harness.waitAt(46); err != nil {
		return err
	}
	for index := range harness.settings.Application {
		harness.settings.Application[index].Recorder.Enable = false
	}
	if err := harness.commit("disable-after-credential-rotation", map[componentKey]string{{"recording", "flv/sample"}: "disabled", {"recording", "ts/sample"}: "disabled"}); err != nil {
		return err
	}
	if err := harness.finishPlayers(players, 1125); err != nil {
		return err
	}
	for app, child := range publishers {
		if err := harness.finish("publish-"+app, child, 70*time.Second); err != nil {
			return err
		}
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	committed, err := validateCredentialServerLog(body)
	if err != nil {
		return err
	}
	trace.mu.Lock()
	uploads, denied := maps.Clone(trace.uploads), append([]credentialOperation(nil), trace.denied...)
	trace.mu.Unlock()
	counts, err := validateCredentialUploads(uploads, committed)
	if err != nil {
		return err
	}
	if len(denied) != 2 || denied[0].Method != http.MethodGet || denied[0].Status != http.StatusForbidden || denied[1].Method != http.MethodPut || denied[1].Status != http.StatusForbidden {
		return fmt.Errorf("unexpected denied requests: %+v", denied)
	}
	harness.results["credential_handoff_counts"], harness.results["credential_commit_time"] = counts, committed
	if err := harness.downloadCredentialRecordings(reader, bucket, uploads); err != nil {
		return err
	}
	if err := harness.checkLogs("publish-*.log", "play-*.log"); err != nil {
		return err
	}
	harness.results["credential_preflight_and_rotation_verified"] = true
	return nil
}

func validateCredentialServerLog(body []byte) (time.Time, error) {
	var committed time.Time
	errorsSeen := map[string]int{}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		var event struct {
			Time  time.Time
			Level string
			Msg   string
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return committed, err
		}
		if event.Msg == "configuration version 2 committed" && committed.IsZero() {
			committed = event.Time
		}
		if event.Level == "WARN" && event.Msg != "Not continuous timestamp" {
			return committed, errors.New("unexpected credential scenario warning")
		}
		if event.Level != "ERROR" {
			continue
		}
		switch event.Msg {
		case "configuration reload rejected: prepare recording: object storage preflight listing failed":
			errorsSeen["listing"]++
		case "configuration reload rejected: prepare recording: object storage preflight write failed":
			errorsSeen["write"]++
		default:
			return committed, fmt.Errorf("unexpected credential scenario error: %s", event.Msg)
		}
	}
	if err := scanner.Err(); err != nil {
		return committed, err
	}
	if committed.IsZero() || errorsSeen["listing"] != 1 || errorsSeen["write"] != 1 {
		return committed, errors.New("missing commit or precise preparation rejections")
	}
	return committed, nil
}

func (harness *harness) downloadCredentialRecordings(reader *s3.Client, bucket string, uploads map[string]credentialUpload) error {
	ctx, cancel := context.WithTimeout(harness.ctx, 30*time.Second)
	defer cancel()
	pending, err := reader.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket})
	if err != nil {
		return err
	}
	if len(pending.Uploads) != 0 || aws.ToBool(pending.IsTruncated) {
		return errors.New("credential matrix left unfinished upload")
	}
	objects, err := reader.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
	if err != nil {
		return err
	}
	if aws.ToBool(objects.IsTruncated) || len(objects.Contents) != len(uploads) {
		return errors.New("recording was overwritten or probe/checkpoint remains")
	}
	for _, object := range objects.Contents {
		key := aws.ToString(object.Key)
		if !filepath.IsLocal(key) || (path.Ext(key) != ".flv" && path.Ext(key) != ".ts") {
			return errors.New("unexpected object after credentials test")
		}
		response, err := reader.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
		if err != nil {
			return err
		}
		filename := filepath.Join(harness.evidence, "recordings", filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
			response.Body.Close()
			return err
		}
		file, err := os.Create(filename)
		if err != nil {
			response.Body.Close()
			return err
		}
		count, copyErr := io.Copy(file, response.Body)
		if err := errors.Join(copyErr, file.Close(), response.Body.Close()); err != nil {
			return err
		}
		if count != aws.ToInt64(object.Size) {
			return errors.New("recording download truncated")
		}
	}
	harness.results["object_cleanup"] = map[string]any{"pending_uploads": 0, "remaining_checkpoints": 0, "completed_recordings": len(objects.Contents)}
	return harness.recordingsWithDuration("recordings", 10, true, 10.5)
}
