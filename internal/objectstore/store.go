package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/internal/mediatime"
)

const MinBlockSize = 5 * 1024 * 1024

type Part struct {
	Number int32
	ETag   string
	Size   int64
}

type Upload struct {
	Key string
	ID  string
}

type Backend interface {
	Create(context.Context, string) (string, error)
	Upload(context.Context, string, string, int32, []byte) (Part, error)
	Parts(context.Context, string, string) ([]Part, error)
	Uploads(context.Context, string) ([]Upload, error)
	Complete(context.Context, string, string, []Part) error
	Abort(context.Context, string, string) error
}

type Client struct {
	Backend          Backend
	transport        *http.Transport
	leaseScope       string
	renew            func() (*Client, error)
	inspectionMu     sync.Mutex
	inspection       *inspectionState
	inspectionClosed bool
	retired          bool
	owners           int
	inspectionDelay  func() time.Duration
}

func (client *Client) Close() {
	if client == nil {
		return
	}
	client.inspectionMu.Lock()
	client.retired = true
	if client.owners != 0 {
		client.inspectionMu.Unlock()
		return
	}
	state := client.stopInspectionLocked()
	client.inspectionMu.Unlock()
	client.finishRetirement(state)
}

func (client *Client) stopInspectionLocked() *inspectionState {
	client.inspectionClosed = true
	state := client.inspection
	if state != nil {
		state.cancel()
	}
	return state
}

func (client *Client) finishRetirement(state *inspectionState) {
	if state != nil {
		state.workers.Wait()
	}
	if client.transport != nil {
		client.transport.CloseIdleConnections()
	}
}

func (client *Client) CleanKey(key string) string {
	return strings.TrimPrefix(path.Clean("/"+key), "/")
}

func ValidateEndpoint(endpoint string) error {
	address, err := url.Parse(endpoint)
	if err != nil || address.Host == "" || (address.Scheme != "http" && address.Scheme != "https") || address.User != nil || address.RawQuery != "" || address.Fragment != "" {
		return errors.New("invalid object storage endpoint")
	}
	return nil
}

func newHTTPClient() (*http.Client, *http.Transport) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 10 * time.Second
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}, transport
}

type MultipartUpload struct {
	client       *Client
	key          string
	id           string
	parts        []Part
	tail         []byte
	checkpointed bool
	media        *mediatime.State
}

func (client *Client) NewMultipartUpload(ctx context.Context, key string, resume bool) (*MultipartUpload, error) {
	key = client.CleanKey(key)
	result := &MultipartUpload{client: client, key: key}
	saved, err := client.loadCheckpoint(ctx, key)
	if err != nil {
		return nil, err
	}
	uploads, err := client.Backend.Uploads(ctx, key)
	if err != nil {
		return nil, err
	}
	var matched *Upload
	for _, upload := range uploads {
		if upload.Key != key {
			continue
		}
		if matched != nil || !resume {
			return nil, errors.New("existing multipart upload is ambiguous or resumption is disabled")
		}
		matched = &upload
	}
	if matched != nil {
		return client.restoreMultipartUpload(ctx, key, matched.ID, saved, false)
	}
	if saved != nil {
		return nil, errors.New("recording checkpoint refers to a missing multipart upload")
	}
	id, err := client.Backend.Create(ctx, key)
	if err != nil {
		return nil, err
	}
	result.id = id
	return result, nil
}

func (client *Client) restoreMultipartUpload(ctx context.Context, key, id string, saved *checkpoint, completing bool) (*MultipartUpload, error) {
	parts, err := client.Backend.Parts(ctx, key, id)
	if err != nil {
		return nil, err
	}
	sort.Slice(parts, func(left, right int) bool { return parts[left].Number < parts[right].Number })
	if completing && saved != nil && saved.ID == id && len(saved.Tail) > 0 && len(parts) == len(saved.Parts)+1 {
		last := parts[len(parts)-1]
		if last.Number == int32(len(parts)) && last.ETag != "" && last.Size == int64(len(saved.Tail)) {
			parts = parts[:len(parts)-1]
		}
	}
	if err := validateParts(parts); err != nil {
		return nil, err
	}
	result := &MultipartUpload{client: client, key: key, id: id, parts: parts}
	if err := result.restore(saved); err != nil {
		return nil, err
	}
	return result, nil
}

func (upload *MultipartUpload) Upload(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	number := int32(1)
	if len(upload.parts) != 0 {
		number = upload.parts[len(upload.parts)-1].Number + 1
	}
	if number > 10000 {
		return errors.New("multipart part limit reached")
	}
	part, err := upload.client.Backend.Upload(ctx, upload.key, upload.id, number, bytes.Clone(data))
	if err != nil {
		return fmt.Errorf("upload part %d: %w", number, err)
	}
	upload.parts = append(upload.parts, part)
	return nil
}

func (upload *MultipartUpload) Complete(ctx context.Context) error {
	if len(upload.parts) == 0 {
		return upload.Abort(ctx)
	}
	if err := upload.client.Backend.Complete(ctx, upload.key, upload.id, upload.parts); err != nil {
		return err
	}
	return upload.clearCheckpoint(ctx)
}

func (upload *MultipartUpload) Abort(ctx context.Context) error {
	if err := upload.client.Backend.Abort(ctx, upload.key, upload.id); err != nil {
		return err
	}
	return upload.clearCheckpoint(ctx)
}
