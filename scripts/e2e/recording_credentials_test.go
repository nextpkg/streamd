package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCredentialUploadEvidenceRejectsReplacementAndMixedSignatures(test *testing.T) {
	committed := time.Unix(20, 0)
	for _, scenario := range []string{"valid", "mixed-signature", "duplicate-key", "unfinished", "missing-data", "old-finished-early", "new-started-early", "missing-stream", "failed-operation"} {
		test.Run(scenario, func(test *testing.T) {
			uploads := map[string]credentialUpload{}
			for _, app := range []string{"flv", "ts"} {
				for index := 0; index < 3; index++ {
					credential, created, completed := "test-next-id", int64(30+index*10), int64(35+index*10)
					if index == 0 {
						credential, created, completed = "test-id", 10, 25
					}
					uploads[fmt.Sprintf("%s-%d", app, index)] = credentialUpload{Key: fmt.Sprintf("%s/sample/%d.%s", app, index, app), Credential: credential, Created: time.Unix(created, 0), Completed: time.Unix(completed, 0), Operations: []credentialOperation{{"POST", credential, 200}, {"PUT", credential, 200}, {"POST", credential, 200}}}
				}
			}
			upload := uploads["flv-0"]
			switch scenario {
			case "mixed-signature":
				upload.Operations[1].Credential = "test-next-id"
			case "duplicate-key":
				upload.Key = uploads["flv-1"].Key
			case "unfinished":
				upload.Completed = time.Time{}
			case "missing-data":
				upload.Operations = append(upload.Operations[:1], upload.Operations[2:]...)
			case "old-finished-early":
				upload.Completed = time.Unix(19, 0)
			case "new-started-early":
				changed := uploads["flv-1"]
				changed.Created = time.Unix(19, 0)
				uploads["flv-1"] = changed
			case "missing-stream":
				delete(uploads, "ts-0")
			case "failed-operation":
				upload.Operations[1].Status = 403
			}
			uploads["flv-0"] = upload
			counts, err := validateCredentialUploads(uploads, committed)
			if scenario == "valid" {
				if err != nil || counts["flv/old-completed-after-commit"] != 1 || counts["ts/test-next-id"] != 2 {
					test.Fatalf("valid evidence rejected: %v %v", counts, err)
				}
			} else if err == nil {
				test.Fatal("invalid credential ownership evidence accepted")
			}
		})
	}
}

func TestCredentialTraceOnlyExemptsNewRecordingCheckpointMisses(test *testing.T) {
	for _, scenario := range []struct {
		name, method, location, code string
		expected                     bool
	}{
		{"new-checkpoint", "GET", "/test-bucket/flv/sample/.streamd-checkpoints/file.json", "NoSuchKey", true},
		{"wrong-location", "GET", "/test-bucket/unrelated/file.json", "NoSuchKey", false},
		{"wrong-code", "GET", "/test-bucket/flv/sample/.streamd-checkpoints/file.json", "AccessDenied", false},
		{"wrong-method", "PUT", "/test-bucket/flv/sample/.streamd-checkpoints/file.json", "NoSuchKey", false},
		{"multipart", "GET", "/test-bucket/flv/sample/.streamd-checkpoints/file.json?uploadId=existing", "NoSuchKey", false},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			request := httptest.NewRequest(scenario.method, scenario.location, nil)
			request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test-id/date/region/service/request")
			response := &http.Response{Request: request, StatusCode: 404, Body: io.NopCloser(strings.NewReader("<Error><Code>" + scenario.code + "</Code></Error>"))}
			trace := &credentialTrace{uploads: map[string]credentialUpload{}}
			if err := trace.response(response); err != nil {
				test.Fatal(err)
			}
			response.Body.Close()
			if scenario.expected {
				if trace.missingCheckpoints != 1 || len(trace.denied) != 0 {
					test.Fatal("normal new checkpoint read rejected")
				}
			} else if trace.missingCheckpoints != 0 || len(trace.denied) != 1 {
				test.Fatal("unrelated failure hidden")
			}
		})
	}
}

func TestCredentialLogsRequireExactRejectionsAndCommit(test *testing.T) {
	base := []map[string]any{
		{"time": time.Unix(20, 0), "level": "INFO", "msg": "configuration version 2 committed"},
		{"time": time.Unix(12, 0), "level": "ERROR", "msg": "configuration reload rejected: prepare recording: object storage preflight listing failed"},
		{"time": time.Unix(32, 0), "level": "ERROR", "msg": "configuration reload rejected: prepare recording: object storage preflight write failed"},
	}
	for _, scenario := range []string{"valid", "missing-rejection", "duplicate-rejection", "extra-error", "extra-warning", "missing-commit"} {
		test.Run(scenario, func(test *testing.T) {
			events := append([]map[string]any(nil), base...)
			switch scenario {
			case "missing-rejection":
				events = events[:2]
			case "duplicate-rejection":
				events = append(events, base[1])
			case "extra-error":
				events = append(events, map[string]any{"level": "ERROR", "msg": "recording failed"})
			case "extra-warning":
				events = append(events, map[string]any{"level": "WARN", "msg": "unknown warning"})
			case "missing-commit":
				events = events[1:]
			}
			var body []byte
			for _, event := range events {
				encoded, err := json.Marshal(event)
				if err != nil {
					test.Fatal(err)
				}
				body = append(body, encoded...)
				body = append(body, '\n')
			}
			_, err := validateCredentialServerLog(body)
			if (err == nil) != (scenario == "valid") {
				test.Fatalf("log verdict: %v", err)
			}
		})
	}
}
