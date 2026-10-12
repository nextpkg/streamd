package objectstore

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Config struct {
	Enable       bool   `json:"enable"`
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	Bucket       string `json:"bucket"`
	Path         string `json:"path"`
	SecretID     string `json:"secret_id"`
	SecretKey    string `json:"secret_key"`
	SessionToken string `json:"session_token"`
	StorageClass string `json:"storage_class"`
	PathStyle    bool   `json:"path_style"`
}

func (cfg *S3Config) Validate() error {
	if cfg == nil || !cfg.Enable {
		return errors.New("S3 is disabled")
	}
	if cfg.Bucket == "" || cfg.SecretID == "" || cfg.SecretKey == "" {
		return errors.New("S3 bucket and credentials are required")
	}
	if cfg.Endpoint != "" {
		return ValidateEndpoint(cfg.Endpoint)
	}
	return nil
}

type s3Backend struct {
	client       *s3.Client
	bucket       string
	storageClass types.StorageClass
}

func NewS3(cfg *S3Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	httpClient, transport := newHTTPClient()
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	options := s3.Options{
		Region: region, HTTPClient: httpClient, UsePathStyle: cfg.PathStyle,
		Credentials:      credentials.NewStaticCredentialsProvider(cfg.SecretID, cfg.SecretKey, cfg.SessionToken),
		RetryMaxAttempts: 2,
	}
	if cfg.Endpoint != "" {
		options.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	snapshot := *cfg
	return &Client{Backend: &s3Backend{client: s3.New(options), bucket: cfg.Bucket, storageClass: types.StorageClass(cfg.StorageClass)}, transport: transport, leaseScope: "s3\x00" + strings.TrimRight(cfg.Endpoint, "/") + "\x00" + cfg.Bucket,
		renew: func() (*Client, error) { return NewS3(&snapshot) }}, nil
}

func (backend *s3Backend) Create(ctx context.Context, key string) (string, error) {
	result, err := backend.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &backend.bucket, Key: &key, StorageClass: backend.storageClass})
	if err != nil {
		return "", err
	}
	return aws.ToString(result.UploadId), nil
}

func (backend *s3Backend) Upload(ctx context.Context, key, id string, number int32, data []byte) (Part, error) {
	result, err := backend.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: &backend.bucket, Key: &key, UploadId: &id, PartNumber: &number, Body: bytes.NewReader(data)})
	if err != nil {
		return Part{}, err
	}
	return Part{Number: number, ETag: aws.ToString(result.ETag), Size: int64(len(data))}, nil
}

func (backend *s3Backend) Parts(ctx context.Context, key, id string) ([]Part, error) {
	paginator := s3.NewListPartsPaginator(backend.client, &s3.ListPartsInput{Bucket: &backend.bucket, Key: &key, UploadId: &id})
	markers := map[string]bool{"": true}
	var parts []Part
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		if aws.ToBool(page.IsTruncated) {
			marker := aws.ToString(page.NextPartNumberMarker)
			if markers[marker] {
				return nil, errors.New("S3 part pagination has a missing or repeated marker")
			}
			markers[marker] = true
		}
		for _, part := range page.Parts {
			parts = append(parts, Part{Number: aws.ToInt32(part.PartNumber), ETag: aws.ToString(part.ETag), Size: aws.ToInt64(part.Size)})
		}
	}
	return parts, nil
}

func (backend *s3Backend) Uploads(ctx context.Context, prefix string) ([]Upload, error) {
	paginator := s3.NewListMultipartUploadsPaginator(backend.client, &s3.ListMultipartUploadsInput{Bucket: &backend.bucket, Prefix: &prefix})
	markers := map[[2]string]bool{}
	var uploads []Upload
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		if aws.ToBool(page.IsTruncated) {
			marker := [2]string{aws.ToString(page.NextKeyMarker), aws.ToString(page.NextUploadIdMarker)}
			if marker[0] == "" || markers[marker] {
				return nil, errors.New("S3 upload pagination has a missing or repeated marker")
			}
			markers[marker] = true
		}
		for _, upload := range page.Uploads {
			uploads = append(uploads, Upload{Key: aws.ToString(upload.Key), ID: aws.ToString(upload.UploadId)})
		}
	}
	return uploads, nil
}

func (backend *s3Backend) Complete(ctx context.Context, key, id string, parts []Part) error {
	completed := make([]types.CompletedPart, 0, len(parts))
	for _, part := range parts {
		completed = append(completed, types.CompletedPart{ETag: aws.String(part.ETag), PartNumber: aws.Int32(part.Number)})
	}
	_, err := backend.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &backend.bucket, Key: &key, UploadId: &id, MultipartUpload: &types.CompletedMultipartUpload{Parts: completed}})
	return err
}

func (backend *s3Backend) Abort(ctx context.Context, key, id string) error {
	_, err := backend.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &backend.bucket, Key: &key, UploadId: &id})
	return err
}

func (backend *s3Backend) ReadCheckpoint(ctx context.Context, key string) ([]byte, error) {
	response, err := backend.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &backend.bucket, Key: &key})
	if err != nil {
		var apiError interface {
			error
			ErrorCode() string
		}
		if errors.As(err, &apiError) && (apiError.ErrorCode() == "NoSuchKey" || apiError.ErrorCode() == "NotFound") {
			return nil, ErrCheckpointNotFound
		}
		return nil, err
	}
	return readCheckpointBody(response.Body)
}

func (backend *s3Backend) WriteCheckpoint(ctx context.Context, key string, data []byte) error {
	_, err := backend.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &backend.bucket, Key: &key, Body: bytes.NewReader(data), ContentType: aws.String("application/json")})
	return err
}

func (backend *s3Backend) DeleteCheckpoint(ctx context.Context, key string) error {
	_, err := backend.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &backend.bucket, Key: &key})
	return err
}
