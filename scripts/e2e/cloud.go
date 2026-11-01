package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
	"github.com/nextpkg/streamd/record/storage/driver"
)

type objectConfig struct {
	Enable    bool   `json:"enable"`
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket,omitempty"`
	SecretID  string `json:"secret_id"`
	SecretKey string `json:"secret_key"`
	PathStyle bool   `json:"path_style,omitempty"`
	Path      string `json:"path,omitempty"`
}

type objectFixture struct {
	provider string
	config   *objectConfig
	server   *teststore.Server
	client   *objectstore.Client
	keys     map[string]string
	reader   *s3.Client
}

func (harness *harness) cloudResume(container bool) (result error) {
	provider := os.Getenv("STREAMD_E2E_OBJECT_PROVIDER")
	if provider == "" {
		provider = "aws"
	}
	if provider != "aws" && provider != "cos" {
		return errors.New("STREAMD_E2E_OBJECT_PROVIDER must be aws or cos")
	}
	if container && provider != "aws" {
		return errors.New("S3-compatible container does not certify the COS protocol")
	}
	bucket := ""
	if provider == "aws" {
		bucket = "test-bucket"
	}
	var server *teststore.Server
	var endpoint string
	if container {
		store, err := harness.startObjectContainer()
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, store.close(harness)) }()
		endpoint = store.endpoint
	} else {
		server = teststore.NewHTTP(bucket)
		defer server.Close()
		endpoint = server.URL
	}
	config := &objectConfig{Enable: true, Endpoint: endpoint, Bucket: bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: provider == "aws"}
	var client *objectstore.Client
	var err error
	if provider == "aws" {
		client, err = objectstore.NewS3(&objectstore.S3Config{Enable: true, Endpoint: config.Endpoint, Bucket: bucket, SecretID: config.SecretID, SecretKey: config.SecretKey, PathStyle: true})
	} else {
		client, err = objectstore.NewCOS(&objectstore.COSConfig{Enable: true, Endpoint: config.Endpoint, SecretID: config.SecretID, SecretKey: config.SecretKey})
	}
	if err != nil {
		return err
	}
	defer client.Close()
	harness.object = &objectFixture{provider: provider, config: config, server: server, client: client, keys: make(map[string]string)}
	harness.results["object_backend"] = provider
	harness.results["object_backend_scope"] = "real public SDK against local HTTP contract server, not an actual vendor or S3-compatible container"
	if container {
		httpClient := &http.Client{Timeout: 30 * time.Second}
		defer httpClient.CloseIdleConnections()
		harness.object.reader = s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: &endpoint, UsePathStyle: true, HTTPClient: httpClient,
			Credentials: credentials.NewStaticCredentialsProvider(config.SecretID, config.SecretKey, ""), RetryMaxAttempts: 1})
		harness.results["object_backend_scope"] = "actual pinned SeaweedFS S3-compatible container; not an AWS or COS vendor certification"
		if err := harness.until("S3 bucket ready", 30*time.Second, func() (bool, error) {
			_, err := harness.object.reader.ListObjectsV2(harness.ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
			return err == nil, nil
		}); err != nil {
			return err
		}
	}
	harness.results["round_finalization"] = "production driver forced completion after third disconnect; time bucket remains open"
	if err := harness.resume(); err != nil {
		return err
	}
	if err := harness.cloudServerErrors(); err != nil {
		return err
	}
	if server != nil {
		if server.PendingUploads() != 0 || len(server.ObjectKeys()) != 2 || len(server.Errors()) != 0 {
			return fmt.Errorf("object cleanup/SDK contract failed: uploads=%d objects=%v errors=%v", server.PendingUploads(), server.ObjectKeys(), server.Errors())
		}
	} else {
		objects, err := harness.object.reader.ListObjectsV2(harness.ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
		if err != nil {
			return err
		}
		uploads, err := client.Backend.Uploads(harness.ctx, "")
		if err != nil {
			return err
		}
		if len(objects.Contents) != 2 || len(uploads) != 0 || aws.ToBool(objects.IsTruncated) {
			return errors.New("container left incomplete uploads or unexpected objects")
		}
		for _, entry := range objects.Contents {
			key := aws.ToString(entry.Key)
			if key != harness.object.keys["flv"] && key != harness.object.keys["ts"] {
				return errors.New("container left a checkpoint object")
			}
		}
	}
	harness.results["object_cleanup"] = map[string]any{"pending_uploads": 0, "remaining_checkpoints": 0, "completed_recordings": 2}
	return nil
}

func (harness *harness) cloudServerErrors() error {
	files, err := filepath.Glob(filepath.Join(harness.evidence, "server*.log"))
	if err != nil {
		return err
	}
	var failures []string
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(bytes.NewReader(body))
		for scanner.Scan() {
			var event struct{ Level, Msg string }
			if err := json.Unmarshal(scanner.Bytes(), &event); err == nil && event.Level == "ERROR" {
				failures = append(failures, event.Msg)
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	harness.results["server_errors"] = failures
	if len(failures) != 0 {
		return fmt.Errorf("cloud recording produced %d server errors: %s", len(failures), failures[0])
	}
	return nil
}

func (harness *harness) objectSnapshot(app string, complete bool) (map[string]any, error) {
	object := harness.object
	uploads, err := object.client.Backend.Uploads(harness.ctx, app+"/sample/")
	if err != nil {
		return nil, err
	}
	if len(uploads) != 1 {
		return nil, fmt.Errorf("%s has %d uploads, expected one continuing round object", app, len(uploads))
	}
	key := uploads[0].Key
	if previous := object.keys[app]; previous != "" && previous != key {
		return nil, errors.New("cloud round resume changed object key")
	}
	object.keys[app] = key
	parts, err := object.client.Backend.Parts(harness.ctx, key, uploads[0].ID)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("cloud fixture %s never crossed a multipart boundary", key)
	}
	state := map[string]any{"key": key, "full_parts": len(parts), "upload_id": uploads[0].ID, "finalized": complete}
	if !complete {
		return state, nil
	}
	writer := driver.NewObject(object.client, key, 3600, true)
	if err := writer.Open(); err != nil {
		return nil, err
	}
	if err := writer.Close(true, true); err != nil {
		return nil, err
	}
	var body io.ReadCloser
	if object.reader != nil {
		response, err := object.reader.GetObject(harness.ctx, &s3.GetObjectInput{Bucket: &object.config.Bucket, Key: &key})
		if err != nil {
			return nil, err
		}
		body = response.Body
	} else {
		url := object.server.URL + "/"
		if object.config.Bucket != "" {
			url += object.config.Bucket + "/"
		}
		response, err := harness.httpClient.Get(url + key)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
			return nil, fmt.Errorf("completed object GET: %d", response.StatusCode)
		}
		body = response.Body
	}
	defer func() { _ = body.Close() }()
	directory := filepath.Join(harness.evidence, "recordings", app)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	file, err := os.Create(filepath.Join(directory, filepath.Base(key)))
	if err != nil {
		return nil, err
	}
	count, copyErr := io.Copy(file, body)
	closeErr := file.Close()
	state["bytes"] = count
	return state, errors.Join(copyErr, closeErr)
}
