package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
)

func TestRecordingPreflightRejectsUnusableCandidateWithoutRevokingActive(test *testing.T) {
	for _, provider := range []string{DriverAws, DriverCos} {
		for _, operation := range []string{"credentials", "write", "read", "delete", "corrupt-read"} {
			test.Run(provider+"/"+operation, func(test *testing.T) {
				bucket := ""
				if provider == DriverAws {
					bucket = "media"
				}
				store := teststore.New(test, bucket)
				backendURL, err := url.Parse(store.URL)
				if err != nil {
					test.Fatal(err)
				}
				proxy := httputil.NewSingleHostReverseProxy(backendURL)
				var enabled atomic.Bool
				enabled.Store(true)
				var requests atomic.Int32
				endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					requests.Add(1)
					object := !request.URL.Query().Has("uploads")
					reject := operation == "credentials" || (object && ((operation == "write" && request.Method == http.MethodPut) || (operation == "read" && request.Method == http.MethodGet) || (operation == "delete" && request.Method == http.MethodDelete)))
					if enabled.Load() && reject {
						writer.WriteHeader(http.StatusForbidden)
						_, _ = fmt.Fprint(writer, "<Error><Code>AccessDenied</Code><Message>secret-do-not-log</Message></Error>") // The HTTP client validates the response; disconnects need no retry.
						return
					}
					if enabled.Load() && operation == "corrupt-read" && object && request.Method == http.MethodGet {
						_, _ = fmt.Fprint(writer, "wrong object content") // The HTTP client validates the response; disconnects need no retry.
						return
					}
					proxy.ServeHTTP(writer, request)
				}))
				defer endpoint.Close()
				filename := filepath.Join(test.TempDir(), "config.json")
				write := func(candidate *Config) {
					test.Helper()
					body, err := json.Marshal(candidate)
					if err != nil {
						test.Fatal(err)
					}
					if err := os.WriteFile(filename, body, 0600); err != nil {
						test.Fatal(err)
					}
				}
				write(NewConfig())
				manager, err := NewManager(context.Background(), filename)
				if err != nil {
					test.Fatal(err)
				}
				defer func() { Get().Revoke() }()
				previous := Get()
				candidate := NewConfig()
				candidate.App[0].Recorder = AppRecorder{Enable: true, SaveAs: AsFlv, SaveMode: ModeRound, Segment: 900, Driver: provider, SaveTo: test.TempDir(), AwsCfg: &objectstore.S3Config{Enable: true, Endpoint: endpoint.URL, Bucket: bucket, SecretID: "test-id", SecretKey: "secret-do-not-log", PathStyle: true, Path: "recordings"}, CosCfg: &objectstore.COSConfig{Enable: true, Endpoint: endpoint.URL, SecretID: "test-id", SecretKey: "secret-do-not-log", Path: "recordings"}}
				write(candidate)
				err = manager.Reload()
				if err == nil {
					test.Fatal("unusable recording candidate committed")
				}
				if strings.Contains(err.Error(), "secret-do-not-log") || strings.Contains(err.Error(), endpoint.URL) {
					test.Fatalf("preflight exposed credentials or endpoint: %v", err)
				}
				if Get() != previous || manager.Version() != 1 || previous.Finder.runtime.ctx.Err() != nil {
					test.Fatal("failed preparation changed or revoked active snapshot")
				}
				if requests.Load() == 0 {
					test.Fatal("no real preflight request")
				}
				if operation != "delete" && len(store.ObjectKeys()) != 0 {
					test.Fatalf("failed preflight left objects: %v", store.ObjectKeys())
				}
				enabled.Store(false)
				if err := manager.Reload(); err != nil {
					test.Fatal(err)
				}
				if manager.Version() != 2 || Get() == previous {
					test.Fatal("repaired target did not commit")
				}
				before := requests.Load()
				candidate.LogLevel = "DEBUG"
				write(candidate)
				if err := manager.Reload(); err != nil {
					test.Fatal(err)
				}
				if requests.Load() != before {
					test.Fatal("unchanged target repeated permission probe")
				}
				if operation == "delete" {
					if len(store.ObjectKeys()) != 1 {
						test.Fatal("unremovable probe was hidden or another probe leaked")
					}
				} else if len(store.ObjectKeys()) != 0 {
					test.Fatal("successful probe left objects")
				}
			})
		}
	}
}

func TestRecordingPreflightSkipsDisabledApplicationsAndRecorders(test *testing.T) {
	var requests atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusForbidden)
	}))
	defer endpoint.Close()
	filename := filepath.Join(test.TempDir(), "config.json")
	candidate := NewConfig()
	candidate.App = []App{
		{Enable: false, AppName: "disabled-app", Recorder: AppRecorder{Enable: true, Driver: DriverAws, AwsCfg: &objectstore.S3Config{Enable: true, Endpoint: endpoint.URL, Bucket: "media", SecretID: "id", SecretKey: "key", PathStyle: true}}},
		{Enable: true, AppName: "disabled-recorder", Recorder: AppRecorder{Enable: false, Driver: DriverCos, CosCfg: &objectstore.COSConfig{Enable: true, Endpoint: endpoint.URL, SecretID: "id", SecretKey: "key"}}},
	}
	body, err := json.Marshal(candidate)
	if err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(filename, body, 0600); err != nil {
		test.Fatal(err)
	}
	manager, err := NewManager(context.Background(), filename)
	if err != nil {
		test.Fatal(err)
	}
	defer Get().Revoke()
	if manager.Version() != 1 || requests.Load() != 0 {
		test.Fatal("disabled storage caused external preparation")
	}
}

func TestRecordingPreparationCancellationJoinsActualHTTPRequest(test *testing.T) {
	for _, provider := range []string{DriverAws, DriverCos} {
		test.Run(provider, func(test *testing.T) {
			entered, cancelled := make(chan struct{}), make(chan struct{})
			endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				close(entered)
				<-request.Context().Done()
				close(cancelled)
			}))
			defer endpoint.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			filename := filepath.Join(test.TempDir(), "config.json")
			if err := os.WriteFile(filename, []byte("{}"), 0600); err != nil {
				test.Fatal(err)
			}
			manager, err := NewManager(ctx, filename)
			if err != nil {
				test.Fatal(err)
			}
			defer Get().Revoke()
			previous := Get()
			candidate := NewConfig()
			candidate.App[0].Recorder = AppRecorder{Enable: true, SaveAs: AsFlv, SaveMode: ModeRound, Segment: 900, Driver: provider, SaveTo: test.TempDir(), AwsCfg: &objectstore.S3Config{Enable: true, Endpoint: endpoint.URL, Bucket: "media", SecretID: "id", SecretKey: "key", PathStyle: true}, CosCfg: &objectstore.COSConfig{Enable: true, Endpoint: endpoint.URL, SecretID: "id", SecretKey: "key"}}
			body, err := json.Marshal(candidate)
			if err != nil {
				test.Fatal(err)
			}
			if err := os.WriteFile(filename, body, 0600); err != nil {
				test.Fatal(err)
			}
			finished := make(chan error, 1)
			go func() { finished <- manager.Reload() }()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				test.Fatal("preparation did not issue HTTP")
			}
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					test.Fatalf("cancellation lost: %v", err)
				}
			case <-time.After(time.Second):
				test.Fatal("cancelled preparation did not retire")
			}
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				test.Fatal("server HTTP request remained active")
			}
			if manager.Version() != 1 || Get() != previous {
				test.Fatal("cancelled candidate committed")
			}
		})
	}
}
