package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nextpkg/streamd/internal/teststore"
)

type storageFaultBackend struct {
	provider   string
	endpoint   string
	bucket     string
	container  *objectContainer
	contract   *teststore.Server
	reader     *s3.Client
	httpClient *http.Client
}

func (harness *harness) startStorageFaultBackend() (backend *storageFaultBackend, result error) {
	provider := os.Getenv("STREAMD_E2E_FAULT_PROVIDER")
	if provider == "" {
		provider = "aws"
	}
	if provider != "aws" && provider != "cos" {
		return nil, errors.New("STREAMD_E2E_FAULT_PROVIDER must be aws or cos")
	}
	backend = &storageFaultBackend{provider: provider}
	harness.results["fault_provider"] = provider
	if provider == "cos" {
		backend.contract = teststore.NewHTTP("")
		backend.endpoint = backend.contract.URL
		harness.results["fault_backend_scope"] = "public COS SDK against local HTTP contract, not a vendor account or S3-compatible implementation of COS"
		return backend, nil
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, backend.close(harness))
		}
	}()
	backend.container, result = harness.startObjectContainer()
	if result != nil {
		return backend, result
	}
	backend.endpoint, backend.bucket = backend.container.endpoint, "test-bucket"
	backend.httpClient = &http.Client{Timeout: 3 * time.Second}
	backend.reader = s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: &backend.endpoint, UsePathStyle: true, HTTPClient: backend.httpClient, Credentials: credentials.NewStaticCredentialsProvider("test-id", "test-secret", ""), RetryMaxAttempts: 1})
	harness.results["fault_backend_scope"] = "public AWS SDK against actual pinned S3-compatible container, not a vendor account"
	result = harness.until("fault storage bucket ready", 30*time.Second, func() (bool, error) {
		_, err := backend.reader.ListObjectsV2(harness.ctx, &s3.ListObjectsV2Input{Bucket: &backend.bucket})
		return err == nil, err
	})
	return backend, result
}

func (backend *storageFaultBackend) close(harness *harness) error {
	if backend.httpClient != nil {
		backend.httpClient.CloseIdleConnections()
	}
	if backend.contract != nil {
		failures := backend.contract.Errors()
		harness.results["fault_contract_errors"] = failures
		backend.contract.Close()
		harness.results["fault_contract_closed"] = true
		if len(failures) > 0 {
			return fmt.Errorf("COS HTTP contract violations: %v", failures)
		}
	}
	if backend.container != nil {
		return backend.container.close(harness)
	}
	return nil
}

func (backend *storageFaultBackend) configure(recorder *recorderConfig, endpoint string) {
	recorder.Driver, recorder.SaveMode, recorder.Segment = backend.provider, "round", 3600
	settings := &objectConfig{Enable: true, Endpoint: endpoint, Bucket: backend.bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: backend.provider == "aws"}
	if backend.provider == "aws" {
		recorder.AWS = settings
	} else {
		recorder.COS = settings
	}
}

func (backend *storageFaultBackend) inventory(harness *harness) (int, []string, error) {
	if backend.contract != nil {
		if failures := backend.contract.Errors(); len(failures) > 0 {
			return 0, nil, fmt.Errorf("COS HTTP contract violations: %v", failures)
		}
		return backend.contract.PendingUploads(), backend.contract.ObjectKeys(), nil
	}
	ctx, cancel := context.WithTimeout(harness.ctx, 5*time.Second)
	defer cancel()
	uploads, err := backend.reader.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &backend.bucket})
	if err != nil {
		return 0, nil, err
	}
	objects, err := backend.reader.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &backend.bucket})
	if err != nil {
		return 0, nil, err
	}
	keys := []string{}
	for _, object := range objects.Contents {
		keys = append(keys, aws.ToString(object.Key))
	}
	return len(uploads.Uploads), keys, nil
}

func uploadErrorMatchesProvider(provider, mode, message string) bool {
	if provider == "aws" {
		return uploadErrorMatches(mode, message)
	}
	if provider != "cos" {
		return false
	}
	if mode == "queue" {
		return strings.Contains(message, "recording upload queue capacity exceeded")
	}
	if !cosFaultOperation(message, "Put", "PUT") {
		return false
	}
	switch mode {
	case "timeout":
		return strings.Contains(message, "timeout awaiting response headers") || strings.Contains(message, "context deadline exceeded")
	case "reset":
		return strings.Contains(message, "connection reset by peer") || strings.Contains(message, "EOF") || strings.Contains(message, "broken pipe")
	case "quota":
		return strings.Contains(message, "507 InsufficientStorage") && strings.Contains(message, "injected capacity exhaustion")
	}
	return false
}

func cosFaultOperation(message, networkMethod, apiMethod string) bool {
	method := strings.Contains(message, networkMethod+" \"http://") || strings.Contains(message, apiMethod+" http://")
	return method && (strings.Contains(message, ".flv?redacted") || strings.Contains(message, ".ts?redacted"))
}
