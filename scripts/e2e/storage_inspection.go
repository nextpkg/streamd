package main

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nextpkg/streamd/internal/objectstore"
)

type inspectionWindow struct {
	Delay       float64 `json:"delay_after_first_create_seconds"`
	Blocked     float64 `json:"blocked_seconds"`
	Completions int     `json:"live_completions_while_blocked"`
	Cancelled   bool    `json:"cancelled"`
}

type inspectionProxy struct {
	server      *httptest.Server
	transport   *http.Transport
	mu          sync.Mutex
	created     map[string]time.Time
	directories map[string]int
	parts       map[string]int
	writes      map[string][]int
	completed   map[string]int
	windows     map[string]inspectionWindow
	release     chan struct{}
}

func newInspectionProxy(endpoint string, hold <-chan struct{}) (*inspectionProxy, error) {
	target, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	state := &inspectionProxy{created: make(map[string]time.Time), directories: make(map[string]int), parts: make(map[string]int),
		completed: make(map[string]int), writes: make(map[string][]int), windows: make(map[string]inspectionWindow), release: make(chan struct{}), transport: http.DefaultTransport.(*http.Transport).Clone()}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = state.transport
	proxy.ModifyResponse = func(response *http.Response) error {
		request := response.Request
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil
		}
		key := strings.TrimPrefix(request.URL.Path, "/test-bucket/")
		prefix := path.Dir(key) + "/"
		state.mu.Lock()
		defer state.mu.Unlock()
		if request.Method == http.MethodPut && request.URL.Query().Get("uploadId") != "" {
			part, err := strconv.Atoi(request.URL.Query().Get("partNumber"))
			if err != nil {
				return err
			}
			state.writes[key] = append(state.writes[key], part)
		}
		if request.Method != http.MethodPost {
			return nil
		}
		if request.URL.Query().Has("uploads") && state.created[prefix].IsZero() {
			state.created[prefix] = time.Now()
		}
		if request.URL.Query().Get("uploadId") != "" {
			state.completed[prefix]++
		}
		return nil
	}
	state.server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		if request.Method == http.MethodGet && query.Get("uploadId") != "" {
			key := strings.TrimPrefix(request.URL.Path, "/test-bucket/")
			state.mu.Lock()
			state.parts[key]++
			state.mu.Unlock()
		}
		prefix := query.Get("prefix")
		if request.Method == http.MethodGet && query.Has("uploads") && strings.HasSuffix(prefix, "/") {
			started := time.Now()
			state.mu.Lock()
			state.directories[prefix]++
			first := state.directories[prefix] == 1
			before, created := state.completed[prefix], state.created[prefix]
			state.mu.Unlock()
			if first {
				cancelled := false
				if hold != nil {
					select {
					case <-hold:
					case <-request.Context().Done():
						cancelled = true
					case <-state.release:
						cancelled = true
					}
				}
				if !cancelled {
					timer := time.NewTimer(6 * time.Second)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-request.Context().Done():
						cancelled = true
					case <-state.release:
						cancelled = true
					}
				}
				state.mu.Lock()
				state.windows[prefix] = inspectionWindow{Delay: started.Sub(created).Seconds(), Blocked: time.Since(started).Seconds(), Completions: state.completed[prefix] - before, Cancelled: cancelled}
				state.mu.Unlock()
				if cancelled {
					return
				}
			}
		}
		proxy.ServeHTTP(response, request)
	}))
	return state, nil
}

func (proxy *inspectionProxy) close() {
	close(proxy.release)
	proxy.server.Close()
	proxy.transport.CloseIdleConnections()
}

func (harness *harness) seedInspectedRecordings(endpoint string) (map[string][]byte, error) {
	seedFLV := filepath.Join(harness.evidence, "inspection-source.flv")
	fixture, err := harness.launch("fixture-inspection-flv", "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-i", filepath.Join(harness.root, "testdata/generated/bframes.flv"), "-t", "3",
		"-vf", "scale=1280:720,noise=alls=24:allf=t+u:all_seed=1", "-c:a", "copy", "-c:v", "libx264", "-preset", "ultrafast", "-g", "25", "-bf", "2",
		"-b:v", "20M", "-minrate", "20M", "-maxrate", "20M", "-bufsize", "20M", "-x264-params", "nal-hrd=cbr:force-cfr=1", seedFLV)
	if err != nil {
		return nil, err
	}
	if err := harness.finish("fixture-inspection-flv", fixture, 20*time.Second); err != nil {
		return nil, err
	}
	seedTS := filepath.Join(harness.evidence, "inspection-source.ts")
	fixture, err = harness.launch("fixture-inspection-ts", "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-i", seedFLV, "-c", "copy", "-bsf:v", "h264_mp4toannexb", "-f", "mpegts", seedTS)
	if err != nil {
		return nil, err
	}
	if err := harness.finish("fixture-inspection-ts", fixture, 20*time.Second); err != nil {
		return nil, err
	}
	client, err := objectstore.NewS3(&objectstore.S3Config{Enable: true, Endpoint: endpoint, Bucket: "test-bucket", SecretID: "test-id", SecretKey: "test-secret", PathStyle: true})
	if err != nil {
		return nil, err
	}
	defer client.Close()
	seeds := make(map[string][]byte)
	for _, kind := range []string{"flv", "ts"} {
		body, err := os.ReadFile(filepath.Join(harness.evidence, "inspection-source."+kind))
		if err != nil {
			return nil, err
		}
		if len(body) <= objectstore.MinBlockSize || len(body) >= 2*objectstore.MinBlockSize {
			return nil, fmt.Errorf("%s inspection fixture must contain one full part plus a tail: %d", kind, len(body))
		}
		key := fmt.Sprintf("%s/sample/%d.%s", kind, time.Now().Add(-time.Hour).Unix(), kind)
		writer, err := client.NewMultipartUpload(harness.ctx, key, true)
		if err != nil {
			return nil, err
		}
		if err := writer.Upload(harness.ctx, body[:objectstore.MinBlockSize]); err != nil {
			return nil, err
		}
		if err := writer.Suspend(harness.ctx, body[objectstore.MinBlockSize:]); err != nil {
			return nil, err
		}
		seeds[key] = body
	}
	return seeds, nil
}

func (harness *harness) storageInspection(removeApplications, retryCompletion bool) (result error) {
	container, err := harness.startObjectContainer()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, container.close(harness)) }()
	httpClient := &http.Client{Timeout: 5 * time.Second}
	defer httpClient.CloseIdleConnections()
	reader := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: &container.endpoint, UsePathStyle: true, HTTPClient: httpClient,
		Credentials: credentials.NewStaticCredentialsProvider("test-id", "test-secret", ""), RetryMaxAttempts: 1})
	bucket := "test-bucket"
	if err := harness.until("S3 bucket ready", 30*time.Second, func() (bool, error) {
		_, err := reader.ListObjectsV2(harness.ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
		return err == nil, nil
	}); err != nil {
		return err
	}
	seeds, err := harness.seedInspectedRecordings(container.endpoint)
	if err != nil {
		return err
	}
	var retryEvidence map[string]inspectionRetryEvidence
	if retryCompletion {
		retryEvidence, err = harness.seedInspectionCompletionFailures(container.endpoint, seeds)
		if err != nil {
			return err
		}
		harness.results["completion_retry_prepared"] = retryEvidence
	}
	var hold chan struct{}
	if removeApplications {
		hold = make(chan struct{})
	}
	proxy, err := newInspectionProxy(container.endpoint, hold)
	if err != nil {
		return err
	}
	defer proxy.close()
	harness.recordingApplications("immediate", 4, 2)
	for index := range harness.settings.Application {
		recorder := &harness.settings.Application[index].Recorder
		recorder.Driver = "aws"
		recorder.AWS = &objectConfig{Enable: true, Endpoint: proxy.server.URL, Bucket: bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: true}
	}
	harness.results["inspection_scope"] = "actual pinned S3-compatible container, original 2..31 second jitter, delayed directory listings, checkpointed full part and tail, concurrent live FLV/TS recording; not vendor or distributed ownership certification"
	if err := harness.start(); err != nil {
		return err
	}
	defer harness.cleanup()
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	players := make(map[string]*process)
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return err
		}
		group, err := harness.players(app, app+"-", 50)
		if err != nil {
			return err
		}
		maps.Copy(players, group)
	}
	watch := harness.watchPlayback(players)
	defer func() { result = errors.Join(result, watch.finish(harness)) }()
	if removeApplications {
		select {
		case <-watch.ready:
		case <-watch.done:
			return fmt.Errorf("playback warmup: %w", watch.err)
		case <-harness.ctx.Done():
			return harness.ctx.Err()
		}
		harness.settings.Application = nil
		if err := harness.commit("remove applications while preserving existing recordings and maintenance", map[componentKey]string{
			{"recording", "flv/sample"}: "retained_for_existing_stream", {"recording", "ts/sample"}: "retained_for_existing_stream",
			{"hls", "flv/sample"}: "retained_for_existing_stream", {"hls", "ts/sample"}: "retained_for_existing_stream",
		}); err != nil {
			return err
		}
		close(hold)
	}
	if err := harness.until("expired recordings finalized asynchronously", 42*time.Second, func() (bool, error) {
		objects, err := reader.ListObjectsV2(harness.ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
		if err != nil {
			return false, err
		}
		found := 0
		for _, object := range objects.Contents {
			if _, exists := seeds[aws.ToString(object.Key)]; exists {
				found++
			}
		}
		return found == len(seeds), nil
	}); err != nil {
		return err
	}
	proxy.mu.Lock()
	windows := maps.Clone(proxy.windows)
	proxy.mu.Unlock()
	harness.results["inspection_windows"] = windows
	for _, prefix := range []string{"flv/sample/", "ts/sample/"} {
		window, exists := windows[prefix]
		if !exists || window.Delay < 1.9 || window.Delay > 33 || window.Blocked < 5.9 || window.Completions < 1 || window.Cancelled {
			return fmt.Errorf("inspection did not preserve jitter and independent live recording for %s: %+v", prefix, window)
		}
	}
	if !removeApplications {
		harness.settings.LogLevel = "DEBUG"
		if err := harness.commit("reload after asynchronous recovery", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
			return err
		}
	}
	if err := harness.finishPlayers(players, 50*25*9/10); err != nil {
		return err
	}
	for name, publisher := range publishers {
		if err := harness.finish("publish-"+name, publisher, 75*time.Second); err != nil {
			return err
		}
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	if err := harness.downloadInspectedRecordings(reader, bucket, seeds); err != nil {
		return err
	}
	proxy.mu.Lock()
	parts, directories := maps.Clone(proxy.parts), maps.Clone(proxy.directories)
	writes := make(map[string][]int)
	for key, parts := range proxy.writes {
		writes[key] = append([]int(nil), parts...)
	}
	proxy.mu.Unlock()
	harness.results["list_parts_requests"], harness.results["directory_list_requests"] = parts, directories
	if len(parts) != len(seeds) {
		return fmt.Errorf("ListParts escaped expired recording recovery: %v", parts)
	}
	for key := range seeds {
		if parts[key] != 1 {
			return fmt.Errorf("expired object %s required %d part listings", key, parts[key])
		}
	}
	if retryCompletion {
		if err := validateInspectionRetryWrites(retryEvidence, writes); err != nil {
			return err
		}
		harness.results["completion_retry_part_writes"] = writes
		harness.results["inspection_completion_retry_verified"] = true
	}
	if err := harness.cloudServerErrors(); err != nil {
		return err
	}
	if err := harness.checkLogs("fixture-*.log", "publish-*.log", "play-*.log"); err != nil {
		return err
	}
	harness.results["storage_inspection_verified"] = true
	if removeApplications {
		harness.results["removed_application_recording_verified"] = true
	}
	return nil
}

func (harness *harness) downloadInspectedRecordings(reader *s3.Client, bucket string, seeds map[string][]byte) error {
	ctx, cancel := context.WithTimeout(harness.ctx, 30*time.Second)
	defer cancel()
	uploads, err := reader.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket})
	if err != nil {
		return err
	}
	objects, err := reader.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
	if err != nil {
		return err
	}
	if len(uploads.Uploads) != 0 || aws.ToBool(uploads.IsTruncated) || aws.ToBool(objects.IsTruncated) {
		return errors.New("inspection left pending uploads or incomplete object listing")
	}
	verified := make(map[string]any)
	for _, object := range objects.Contents {
		key := aws.ToString(object.Key)
		if path.Ext(key) != ".flv" && path.Ext(key) != ".ts" {
			return fmt.Errorf("inspection left checkpoint or unexpected object %s", key)
		}
		response, err := reader.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(response.Body)
		if err := errors.Join(readErr, response.Body.Close()); err != nil {
			return err
		}
		if expected, exists := seeds[key]; exists {
			if !bytes.Equal(body, expected) {
				return fmt.Errorf("inspection lost or altered original bytes for %s", key)
			}
			verified[key] = map[string]any{"bytes": len(body), "sha256": fmt.Sprintf("%x", sha256.Sum256(body)), "matches_original": true, "full_parts": len(body) / objectstore.MinBlockSize, "tail_bytes": len(body) % objectstore.MinBlockSize}
		}
		filename := filepath.Join(harness.evidence, "recordings", filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
			return err
		}
		if err := os.WriteFile(filename, body, 0600); err != nil {
			return err
		}
	}
	if len(verified) != len(seeds) {
		return errors.New("inspection did not complete every seeded recording")
	}
	harness.results["recovered_recordings"] = verified
	harness.results["object_cleanup"] = map[string]any{"pending_uploads": 0, "remaining_checkpoints": 0, "completed_recordings": len(objects.Contents)}
	return harness.recordings("recordings", 28, true)
}
