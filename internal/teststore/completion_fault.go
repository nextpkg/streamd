package teststore

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type PartDigest struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type CompletionOutcome struct {
	Created                  int                `json:"created"`
	SuccessfulCompletions    int                `json:"successful_completion_responses"`
	CompletionManifestSHA256 string             `json:"completion_manifest_sha256"`
	Dropped                  int                `json:"dropped"`
	Denied                   int                `json:"denied_checkpoint_delete"`
	Aborted                  int                `json:"aborted"`
	UploadID                 string             `json:"upload_id"`
	Parts                    map[int]PartDigest `json:"parts"`
}

type CompletionFault struct {
	*httptest.Server
	mu        sync.Mutex
	mode      string
	bucket    string
	targets   map[string]bool
	deletes   map[string]string
	outcomes  map[string]CompletionOutcome
	transport *http.Transport
	failures  []string
}

var errCompletionResponseDropped = errors.New("injected connection close after upstream completion")

func NewCompletionFault(endpoint, bucket, mode string) (*CompletionFault, error) {
	if mode != "response-lost" && mode != "cleanup-denied" {
		return nil, errors.New("unknown completion fault mode")
	}
	target, err := url.Parse(endpoint)
	if err != nil || target.Scheme != "http" || target.Host == "" {
		return nil, errors.New("invalid local completion fault endpoint")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 10 * time.Second
	fault := &CompletionFault{mode: mode, bucket: bucket, targets: map[string]bool{}, deletes: map[string]string{}, outcomes: map[string]CompletionOutcome{}, transport: transport}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = fault
	proxy.ErrorHandler = func(response http.ResponseWriter, request *http.Request, err error) {
		if errors.Is(err, errCompletionResponseDropped) {
			connection, _, hijackErr := response.(http.Hijacker).Hijack()
			if hijackErr == nil {
				_ = connection.Close()
				return
			}
			err = hijackErr
		}
		fault.mu.Lock()
		fault.failures = append(fault.failures, err.Error())
		fault.mu.Unlock()
		http.Error(response, "unexpected test proxy failure", http.StatusBadGateway)
	}
	fault.Server = httptest.NewServer(proxy)
	return fault, nil
}

func (fault *CompletionFault) Close() {
	fault.Server.Close()
	fault.transport.CloseIdleConnections()
}

func (fault *CompletionFault) Arm(keys []string) error {
	fault.mu.Lock()
	defer fault.mu.Unlock()
	for _, key := range keys {
		if fault.outcomes[key].Created != 1 || fault.outcomes[key].SuccessfulCompletions != 0 || fault.targets[key] {
			return fmt.Errorf("fault target is not one existing uncompleted upload: %s", key)
		}
	}
	for _, key := range keys {
		fault.targets[key] = true
		digest := sha256.Sum256([]byte(key))
		fault.deletes[path.Join(path.Dir(key), ".streamd-checkpoints", fmt.Sprintf("%x.json", digest))] = key
	}
	return nil
}

func (fault *CompletionFault) Snapshot() (map[string]CompletionOutcome, []string) {
	fault.mu.Lock()
	defer fault.mu.Unlock()
	result := maps.Clone(fault.outcomes)
	for key, outcome := range result {
		outcome.Parts = maps.Clone(outcome.Parts)
		result[key] = outcome
	}
	return result, append([]string(nil), fault.failures...)
}

func (fault *CompletionFault) RoundTrip(request *http.Request) (*http.Response, error) {
	key := strings.TrimPrefix(request.URL.Path, "/")
	if fault.bucket != "" {
		key = strings.TrimPrefix(key, fault.bucket+"/")
	}
	query := request.URL.Query()
	fault.mu.Lock()
	if target := fault.deletes[key]; fault.mode == "cleanup-denied" && target != "" && request.Method == http.MethodDelete && query.Get("uploadId") == "" {
		outcome := fault.outcomes[target]
		outcome.Denied++
		fault.outcomes[target] = outcome
		fault.mu.Unlock()
		body := `<Error><Code>AccessDenied</Code><Message>injected checkpoint cleanup denial</Message></Error>`
		return &http.Response{StatusCode: http.StatusForbidden, Status: "403 Forbidden", Header: http.Header{"Content-Type": []string{"application/xml"}}, Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: request}, nil
	}
	fault.mu.Unlock()
	var part PartDigest
	var completionManifest string
	number := 0
	if request.Method == http.MethodPut && query.Get("uploadId") != "" {
		var err error
		number, err = strconv.Atoi(query.Get("partNumber"))
		if err != nil || number < 1 || number > 10000 {
			return nil, errors.New("invalid captured part number")
		}
		body, readErr := io.ReadAll(io.LimitReader(request.Body, 5*1024*1024+1))
		if err := errors.Join(readErr, request.Body.Close()); err != nil {
			return nil, err
		}
		if len(body) == 0 || len(body) > 5*1024*1024 {
			return nil, errors.New("captured part outside fixture bounds")
		}
		part = PartDigest{Bytes: int64(len(body)), SHA256: fmt.Sprintf("%x", sha256.Sum256(body))}
		request.Body = io.NopCloser(bytes.NewReader(body))
	}
	if request.Method == http.MethodPost && query.Get("uploadId") != "" {
		body, readErr := io.ReadAll(io.LimitReader(request.Body, 2*1024*1024+1))
		if err := errors.Join(readErr, request.Body.Close()); err != nil {
			return nil, err
		}
		if len(body) == 0 || len(body) > 2*1024*1024 {
			return nil, errors.New("completion manifest outside fixture bounds")
		}
		completionManifest = fmt.Sprintf("%x", sha256.Sum256(body))
		request.Body = io.NopCloser(bytes.NewReader(body))
	}
	response, err := fault.transport.RoundTrip(request)
	if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		return response, err
	}
	fault.mu.Lock()
	defer fault.mu.Unlock()
	outcome := fault.outcomes[key]
	changed := false
	if request.Method == http.MethodPost && query.Has("uploads") {
		outcome.Created++
		changed = true
	}
	if id := query.Get("uploadId"); id != "" {
		if outcome.UploadID != "" && outcome.UploadID != id {
			fault.failures = append(fault.failures, "upload identity changed for "+key)
		}
		outcome.UploadID = id
		switch request.Method {
		case http.MethodPut:
			if outcome.Parts == nil {
				outcome.Parts = map[int]PartDigest{}
			}
			outcome.Parts[number] = part
			changed = true
		case http.MethodPost:
			if outcome.CompletionManifestSHA256 != "" && outcome.CompletionManifestSHA256 != completionManifest {
				fault.failures = append(fault.failures, "completion retry changed manifest for "+key)
			}
			outcome.CompletionManifestSHA256 = completionManifest
			outcome.SuccessfulCompletions++
			changed = true
			if fault.mode == "response-lost" && fault.targets[key] {
				_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 1024*1024))
				closeErr := response.Body.Close()
				if err := errors.Join(readErr, closeErr); err != nil {
					return nil, err
				}
				outcome.Dropped++
				fault.outcomes[key] = outcome
				return nil, errCompletionResponseDropped
			}
		case http.MethodDelete:
			outcome.Aborted++
			changed = true
		}
	}
	if changed {
		fault.outcomes[key] = outcome
	}
	return response, nil
}

func VerifyCompletedParts(body []byte, parts map[int]PartDigest) error {
	if len(parts) == 0 {
		return errors.New("missing independent uploaded parts")
	}
	offset := int64(0)
	for number := 1; number <= len(parts); number++ {
		part, exists := parts[number]
		if !exists || part.Bytes <= 0 || part.Bytes > int64(len(body))-offset {
			return errors.New("invalid completed part extent")
		}
		end := offset + part.Bytes
		if fmt.Sprintf("%x", sha256.Sum256(body[offset:end])) != part.SHA256 {
			return fmt.Errorf("completed part %d payload differs", number)
		}
		offset = end
	}
	if offset != int64(len(body)) {
		return errors.New("completed object has unaccounted bytes")
	}
	return nil
}
