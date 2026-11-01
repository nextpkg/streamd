package objectstore

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

type COSConfig struct {
	Enable       bool   `json:"enable"`
	Address      string `json:"address"`
	Endpoint     string `json:"endpoint"`
	Path         string `json:"path"`
	SecretID     string `json:"secret_id"`
	SecretKey    string `json:"secret_key"`
	SessionToken string `json:"session_token"`
	StorageClass string `json:"storage_class"`
}

func (cfg *COSConfig) endpoint() string {
	if cfg.Endpoint != "" {
		return cfg.Endpoint
	}
	bucket, region, found := strings.Cut(cfg.Address, ".")
	if !found {
		return ""
	}
	return "https://" + bucket + ".cos." + region + ".myqcloud.com"
}

func (cfg *COSConfig) Validate() error {
	if cfg == nil || !cfg.Enable {
		return errors.New("COS is disabled")
	}
	if cfg.SecretID == "" || cfg.SecretKey == "" {
		return errors.New("COS credentials are required")
	}
	return ValidateEndpoint(cfg.endpoint())
}

type cosBackend struct {
	client       *cos.Client
	storageClass string
}

func NewCOS(cfg *COSConfig) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	httpClient, transport := newHTTPClient()
	httpClient.Transport = &cos.AuthorizationTransport{SecretID: cfg.SecretID, SecretKey: cfg.SecretKey, SessionToken: cfg.SessionToken, Transport: transport}
	endpoint, _ := url.Parse(cfg.endpoint())
	snapshot := *cfg
	return &Client{Backend: &cosBackend{client: cos.NewClient(&cos.BaseURL{BucketURL: endpoint}, httpClient), storageClass: cfg.StorageClass}, transport: transport, leaseScope: "cos\x00" + strings.TrimRight(endpoint.String(), "/"),
		renew: func() (*Client, error) { return NewCOS(&snapshot) }}, nil
}

func (backend *cosBackend) Create(ctx context.Context, key string) (string, error) {
	result, _, err := backend.client.Object.InitiateMultipartUpload(ctx, key, &cos.InitiateMultipartUploadOptions{ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{XCosStorageClass: backend.storageClass}})
	if err != nil {
		return "", err
	}
	return result.UploadID, nil
}

func (backend *cosBackend) Upload(ctx context.Context, key, id string, number int32, data []byte) (Part, error) {
	response, err := backend.client.Object.UploadPart(ctx, key, id, int(number), bytes.NewReader(data), &cos.ObjectUploadPartOptions{ContentLength: int64(len(data))})
	if err != nil {
		return Part{}, err
	}
	return Part{Number: number, ETag: response.Header.Get("ETag"), Size: int64(len(data))}, nil
}

func (backend *cosBackend) Parts(ctx context.Context, key, id string) ([]Part, error) {
	options := &cos.ObjectListPartsOptions{}
	markers := map[string]bool{"": true}
	var parts []Part
	for {
		page, _, err := backend.client.Object.ListParts(ctx, key, id, options)
		if err != nil {
			return nil, err
		}
		for _, part := range page.Parts {
			parts = append(parts, Part{Number: int32(part.PartNumber), ETag: part.ETag, Size: int64(part.Size)})
		}
		if !page.IsTruncated {
			break
		}
		if markers[page.NextPartNumberMarker] {
			return nil, errors.New("COS part pagination has a missing or repeated marker")
		}
		markers[page.NextPartNumberMarker] = true
		options.PartNumberMarker = page.NextPartNumberMarker
	}
	return parts, nil
}

func (backend *cosBackend) Uploads(ctx context.Context, prefix string) ([]Upload, error) {
	options := &cos.ListMultipartUploadsOptions{Prefix: prefix}
	markers := map[[2]string]bool{}
	var uploads []Upload
	for {
		page, _, err := backend.client.Bucket.ListMultipartUploads(ctx, options)
		if err != nil {
			return nil, err
		}
		for _, upload := range page.Uploads {
			uploads = append(uploads, Upload{Key: upload.Key, ID: upload.UploadID})
		}
		if !page.IsTruncated {
			break
		}
		marker := [2]string{page.NextKeyMarker, page.NextUploadIDMarker}
		if marker[0] == "" || markers[marker] {
			return nil, errors.New("COS upload pagination has a missing or repeated marker")
		}
		markers[marker] = true
		options.KeyMarker, options.UploadIDMarker = page.NextKeyMarker, page.NextUploadIDMarker
	}
	return uploads, nil
}

func (backend *cosBackend) Complete(ctx context.Context, key, id string, parts []Part) error {
	options := &cos.CompleteMultipartUploadOptions{}
	for _, part := range parts {
		options.Parts = append(options.Parts, cos.Object{PartNumber: int(part.Number), ETag: part.ETag})
	}
	_, _, err := backend.client.Object.CompleteMultipartUpload(ctx, key, id, options)
	return err
}

func (backend *cosBackend) Abort(ctx context.Context, key, id string) error {
	_, err := backend.client.Object.AbortMultipartUpload(ctx, key, id)
	return err
}

func (backend *cosBackend) ReadCheckpoint(ctx context.Context, key string) ([]byte, error) {
	response, err := backend.client.Object.Get(ctx, key, nil)
	if err != nil {
		var apiError *cos.ErrorResponse
		if errors.As(err, &apiError) && apiError.Code == "NoSuchKey" {
			return nil, ErrCheckpointNotFound
		}
		return nil, err
	}
	return readCheckpointBody(response.Body)
}

func (backend *cosBackend) WriteCheckpoint(ctx context.Context, key string, data []byte) error {
	_, err := backend.client.Object.Put(ctx, key, bytes.NewReader(data), nil)
	return err
}

func (backend *cosBackend) DeleteCheckpoint(ctx context.Context, key string) error {
	_, err := backend.client.Object.Delete(ctx, key)
	return err
}

var _ http.RoundTripper = (*cos.AuthorizationTransport)(nil)
