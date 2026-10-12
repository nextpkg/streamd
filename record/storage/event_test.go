package storage

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
)

type callbackCompletionDriver struct {
	failure   error
	completed bool
	closed    int
}

func (driver *callbackCompletionDriver) Open() error        { return nil }
func (driver *callbackCompletionDriver) Write([]byte) error { return nil }
func (driver *callbackCompletionDriver) Close(complete, block bool) error {
	driver.completed = complete
	driver.closed++
	return driver.failure
}

func TestRecordingCallbacksFollowSuccessfulFileLifecycle(t *testing.T) {
	for _, scenario := range []string{"success", "open-failure", "write-failure"} {
		t.Run(scenario, func(t *testing.T) {
			started, completed := 0, 0
			directory := t.TempDir()
			if scenario == "open-failure" {
				directory = filepath.Join(directory, "file-not-directory")
				if err := os.WriteFile(directory, []byte("preserve"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			store := NewStorage(base.NewInfo("events", "sample", false), &config.AppRecorder{SaveTo: directory, SaveAs: "flv", SaveMode: "immediate", Segment: 4, Driver: "fs"}, &Event{OnRecord: func(string) { started++ }, OnRecordDone: func(string) { completed++ }})
			defer store.Release()
			openError := store.Open()
			if scenario == "open-failure" {
				if openError == nil || started != 0 || completed != 0 {
					t.Fatal("failed open emitted a successful lifecycle event")
				}
				if err := store.Close(true); err != nil || completed != 0 {
					t.Fatal("closing an unopened recording emitted completion")
				}
				return
			}
			if openError != nil || started != 1 {
				t.Fatalf("successful open callback count=%d: %v", started, openError)
			}
			if err := store.Open(); err != nil || started != 1 {
				t.Fatal("idempotent open repeated callback")
			}
			if scenario == "write-failure" {
				if err := store.dr.Close(false, true); err != nil {
					t.Fatal(err)
				}
				if err := store.Write([]byte("cannot-write-closed-descriptor")); err == nil {
					t.Fatal("fault injection did not produce an actual filesystem write error")
				}
				if err := store.Close(true); err == nil || completed != 0 {
					t.Fatal("failed recording emitted successful completion")
				}
			} else {
				if err := store.Write([]byte("test-payload")); err != nil {
					t.Fatal(err)
				}
				if err := store.Close(true); err != nil || completed != 1 {
					t.Fatalf("successful completion callback count=%d: %v", completed, err)
				}
			}
			previous := completed
			if err := store.Close(true); err != nil || completed != previous {
				t.Fatal("idempotent close repeated callback")
			}
		})
	}
}

func TestRecordingCompletionCallbackExcludesPauseAndFinalizerFailure(t *testing.T) {
	for _, scenario := range []string{"pause", "complete", "failure"} {
		t.Run(scenario, func(t *testing.T) {
			completed := 0
			settings := &config.AppRecorder{SaveAs: "flv", SaveMode: "round", Segment: 900, Driver: "aws"}
			store := NewStorage(base.NewInfo("events", "sample", false), settings, &Event{OnRecord: func(string) {}, OnRecordDone: func(string) { completed++ }})
			defer store.Release()
			driver := &callbackCompletionDriver{}
			if scenario == "failure" {
				driver.failure = errors.New("finalizer failed")
			}
			store.dr = driver
			store.isOpen.Store(true)
			online.Add(1)
			store.nTime = time.Unix(settings.Normalize(time.Now().Unix()), 0)
			if scenario != "pause" {
				store.ForceCompletion()
			}
			err := store.Close(true)
			if (err != nil) != (scenario == "failure") || driver.completed != (scenario != "pause") || driver.closed != 1 || completed != map[string]int{"pause": 0, "complete": 1, "failure": 0}[scenario] {
				t.Fatalf("wrong completion decision: scenario=%s error=%v driver=%+v events=%d", scenario, err, driver, completed)
			}
		})
	}
}

func TestRecordingCallbackPayload(t *testing.T) {
	connection := &base.ConnectInfo{App: "app", FlashVer: "independent-fixture", TcURL: "rtmp://127.0.0.1/app", AudioCodecs: 1024, VideoCodecs: 128, FPad: true}
	expectedConnection := map[string]any{"app": "app", "flashVer": "independent-fixture", "swfUrl": "", "tcUrl": "rtmp://127.0.0.1/app", "fpad": true, "audioCodecs": float64(1024), "videoCodecs": float64(128), "videoFunction": float64(0), "pageUrl": "", "objectEncoding": float64(0)}
	for _, name := range []string{"OnRecord", "OnRecordDone"} {
		t.Run(name, func(t *testing.T) {
			observed := make(chan map[string]any, 1)
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				var payload map[string]any
				if request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" || json.NewDecoder(request.Body).Decode(&payload) != nil {
					observed <- nil
					return
				}
				observed <- payload
			}))
			defer server.Close()
			before := time.Now().UnixMilli()
			FireEvent(name, server.URL, "app/stream", connection)("/app/stream/recording.flv")
			select {
			case payload := <-observed:
				if len(payload) != 6 || payload["event"] != name || payload["key"] != "app/stream" || payload["filename"] != "recording.flv" || payload["location"] != "/app/stream/recording.flv" || !reflect.DeepEqual(payload["connInfo"], expectedConnection) {
					t.Fatalf("recording callback contract changed: %v", payload)
				}
				occur, valid := payload["occur"].(float64)
				if !valid || occur < float64(before) || occur > float64(time.Now().UnixMilli()) {
					t.Fatalf("invalid millisecond event timestamp: %v", payload)
				}
			case <-time.After(time.Second):
				t.Fatal("recording callback not delivered")
			}
		})
	}
}
