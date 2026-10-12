package storage

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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
)

func TestCloudRotationBoundsOutstandingObjectsAcrossStores(test *testing.T) {
	for _, provider := range []string{"aws", "cos"} {
		test.Run(provider, func(test *testing.T) {
			bucket := ""
			if provider == "aws" {
				bucket = "bucket"
			}
			backend := teststore.New(test, bucket)
			address, err := url.Parse(backend.URL)
			if err != nil {
				test.Fatal(err)
			}
			proxy := httputil.NewSingleHostReverseProxy(address)
			entered, gate := make(chan struct{}, 8), make(chan struct{})
			var unlockOnce sync.Once
			unlock := func() { unlockOnce.Do(func() { close(gate) }) }
			endpoint := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.Method == http.MethodPost && request.URL.Query().Has("uploadId") {
					entered <- struct{}{}
					select {
					case <-gate:
					case <-request.Context().Done():
						return
					}
				}
				proxy.ServeHTTP(response, request)
			}))
			var stores []*Storage
			test.Cleanup(func() {
				unlock()
				for _, store := range stores {
					_ = store.Close(true)
					store.Release()
				}
				config.Get().Revoke()
				endpoint.Close()
			})
			settings := config.NewConfig()
			recorder := config.AppRecorder{Enable: true, SaveTo: test.TempDir(), SaveAs: "flv", SaveMode: "round", Segment: 86400, Driver: provider}
			if provider == "aws" {
				recorder.AwsCfg = &objectstore.S3Config{Enable: true, Endpoint: endpoint.URL, Bucket: bucket, SecretID: "test-id", SecretKey: "test-secret", PathStyle: true}
			} else {
				recorder.CosCfg = &objectstore.COSConfig{Enable: true, Endpoint: endpoint.URL, SecretID: "test-id", SecretKey: "test-secret"}
			}
			settings.App = []config.App{{Enable: true, AppName: "rotation", Recorder: recorder}}
			body, err := json.Marshal(settings)
			if err != nil {
				test.Fatal(err)
			}
			filename := filepath.Join(test.TempDir(), "config.json")
			if err := os.WriteFile(filename, body, 0600); err != nil {
				test.Fatal(err)
			}
			if _, err := config.NewManager(context.Background(), filename); err != nil {
				test.Fatal(err)
			}
			budget := NewFinalizationBudget()
			var completed atomic.Int64
			create := func(instance string) *Storage {
				store := NewStorage(base.NewInfo("rotation", instance, false), config.Get().GetRecorder("rotation"), &Event{OnRecord: func(string) {}, OnRecordDone: func(string) { completed.Add(1) }})
				store.SetFinalizationBudget(budget)
				stores = append(stores, store)
				return store
			}
			for index := 0; index < 4; index++ {
				store := create(fmt.Sprintf("stream%d", index))
				if err := store.Open(); err != nil {
					test.Fatal(err)
				}
				if err := store.Write([]byte("confirmed payload")); err != nil {
					test.Fatal(err)
				}
				store.ForceCompletion()
				if err := store.Close(false); err != nil {
					test.Fatal(err)
				}
				select {
				case <-entered:
				case <-time.After(time.Second):
					test.Fatal("cloud completion did not reach the independent gate")
				}
				if index == 0 {
					duplicate := create("stream0")
					storeKey := store.fullRela
					if err := duplicate.Open(); !errors.Is(err, ErrFinalizationPending) || duplicate.fullRela != storeKey {
						test.Fatalf("same key reopened while its old completion is pending: %v", err)
					}
				}
			}
			extra := create("overflow")
			if err := extra.Open(); !errors.Is(err, ErrFinalizationPending) || !extra.WaitingFinalization() || extra.IsOpened() {
				test.Fatalf("unbounded cloud admission: %v", err)
			}
			if backend.PendingUploads() != 4 || completed.Load() != 0 || len(backend.ObjectKeys()) != 0 {
				test.Fatal("pending objects or premature callbacks differ from the four admitted uploads")
			}
			unlock()
			for _, store := range stores {
				if err := store.Close(true); err != nil {
					test.Fatal(err)
				}
			}
			if completed.Load() != 4 || backend.PendingUploads() != 0 || len(backend.ObjectKeys()) != 4 {
				test.Fatal("accepted cloud completions were not joined exactly once")
			}
			if err := extra.Open(); err != nil || extra.WaitingFinalization() {
				test.Fatalf("retired objects did not release capacity: %v", err)
			}
			if err := extra.Write([]byte("recovered")); err != nil {
				test.Fatal(err)
			}
			extra.ForceCompletion()
			if err := extra.Close(true); err != nil || completed.Load() != 5 {
				test.Fatalf("recovered object did not complete: %v", err)
			}
		})
	}
}
