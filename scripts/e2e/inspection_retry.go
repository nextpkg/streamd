package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/record/storage/driver"
)

type inspectionRetryEvidence struct {
	UploadID            string `json:"upload_id"`
	CheckpointSHA256    string `json:"checkpoint_sha256"`
	FullPartETag        string `json:"full_part_etag"`
	TailBytes           int    `json:"tail_bytes"`
	RejectedCompletions int    `json:"rejected_completions"`
}

func (harness *harness) seedInspectionCompletionFailures(endpoint string, seeds map[string][]byte) (map[string]inspectionRetryEvidence, error) {
	target, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = transport
	var mu sync.Mutex
	rejected := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		key := strings.TrimPrefix(request.URL.Path, "/test-bucket/")
		if request.Method == http.MethodPost && request.URL.Query().Get("uploadId") != "" {
			if _, exists := seeds[key]; exists {
				mu.Lock()
				rejected[key]++
				mu.Unlock()
				response.Header().Set("Content-Type", "application/xml")
				response.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(response, `<Error><Code>AccessDenied</Code><Message>injected inspection completion failure</Message></Error>`)
				return
			}
		}
		proxy.ServeHTTP(response, request)
	}))
	defer server.Close()
	client, err := objectstore.NewS3(&objectstore.S3Config{Enable: true, Endpoint: server.URL, Bucket: "test-bucket", SecretID: "test-id", SecretKey: "test-secret", PathStyle: true})
	if err != nil {
		return nil, err
	}
	defer client.Close()
	checkpoints := client.Backend.(interface {
		ReadCheckpoint(context.Context, string) ([]byte, error)
	})
	evidence := map[string]inspectionRetryEvidence{}
	for key, body := range seeds {
		checkpointKey := path.Join(path.Dir(key), ".streamd-checkpoints", fmt.Sprintf("%x.json", sha256.Sum256([]byte(key))))
		saved, err := checkpoints.ReadCheckpoint(harness.ctx, checkpointKey)
		if err != nil {
			return nil, err
		}
		uploads, err := client.Backend.Uploads(harness.ctx, key)
		if err != nil || len(uploads) != 1 || uploads[0].Key != key {
			return nil, fmt.Errorf("seed upload is ambiguous: %v %v", uploads, err)
		}
		before, err := client.Backend.Parts(harness.ctx, key, uploads[0].ID)
		if err != nil || len(before) != 1 || before[0].Number != 1 || before[0].Size != objectstore.MinBlockSize {
			return nil, fmt.Errorf("seed full part invalid: %v %v", before, err)
		}
		result, err := client.InspectExpired(harness.ctx, key, 4, driver.IsExpired)
		var apiError interface {
			error
			ErrorCode() string
		}
		if !errors.As(err, &apiError) || apiError.ErrorCode() != "AccessDenied" || result.Finalized != 0 {
			return nil, fmt.Errorf("completion failure was not observed: %+v %v", result, err)
		}
		current, err := checkpoints.ReadCheckpoint(harness.ctx, checkpointKey)
		if err != nil || !bytes.Equal(saved, current) {
			return nil, fmt.Errorf("failed completion changed checkpoint: %v", err)
		}
		after, err := client.Backend.Parts(harness.ctx, key, uploads[0].ID)
		if err != nil || len(after) != 2 || after[0] != before[0] || after[1].Number != 2 || after[1].Size != int64(len(body)-objectstore.MinBlockSize) {
			return nil, fmt.Errorf("failed completion did not retain full part plus provisional tail: %v %v", after, err)
		}
		mu.Lock()
		count := rejected[key]
		mu.Unlock()
		if count != 1 {
			return nil, fmt.Errorf("unexpected rejected completion count for %s: %d", key, count)
		}
		evidence[key] = inspectionRetryEvidence{UploadID: uploads[0].ID, CheckpointSHA256: fmt.Sprintf("%x", sha256.Sum256(saved)), FullPartETag: before[0].ETag, TailBytes: len(body) - objectstore.MinBlockSize, RejectedCompletions: count}
	}
	return evidence, nil
}

func validateInspectionRetryWrites(prepared map[string]inspectionRetryEvidence, writes map[string][]int) error {
	if len(prepared) != 2 {
		return errors.New("missing FLV/TS failed-completion seeds")
	}
	for key, evidence := range prepared {
		if evidence.UploadID == "" || evidence.CheckpointSHA256 == "" || evidence.FullPartETag == "" || evidence.TailBytes <= 0 || evidence.TailBytes >= objectstore.MinBlockSize || evidence.RejectedCompletions != 1 {
			return errors.New("incomplete failed-completion evidence")
		}
		if len(writes[key]) != 1 || writes[key][0] != 2 {
			return fmt.Errorf("recovery did not overwrite exactly the provisional tail part for %s: %v", key, writes[key])
		}
	}
	return nil
}
