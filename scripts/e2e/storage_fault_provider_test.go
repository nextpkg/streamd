package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCOSFaultLogsRequireUploadOperationAndCause(test *testing.T) {
	samples := map[string]string{
		"queue":   "recording upload queue capacity exceeded",
		"timeout": "Put \"http://127.0.0.1:9000/flv%2Fsample%2F1.flv?redacted\": net/http: timeout awaiting response headers",
		"reset":   "Put \"http://127.0.0.1:9000/flv%2Fsample%2F1.flv?redacted\": read: connection reset by peer",
		"quota":   "PUT http://127.0.0.1:9000/flv/sample/1.flv?redacted 507 InsufficientStorage(Message: injected capacity exhaustion)",
	}
	for mode, message := range samples {
		if !uploadErrorMatchesProvider("cos", mode, message) {
			test.Fatalf("COS %s rejected", mode)
		}
		if uploadErrorMatchesProvider("unknown", mode, message) {
			test.Fatal("unknown provider accepted")
		}
		if mode != "queue" && uploadErrorMatchesProvider("aws", mode, message) {
			test.Fatal("COS evidence accepted as AWS")
		}
	}
	for _, message := range []string{"subscriber EOF", samples["queue"], strings.ReplaceAll(samples["reset"], "Put ", "Get "), strings.ReplaceAll(samples["reset"], "1.flv?redacted", "checkpoint.json?redacted"), "operation error S3: UploadPart: EOF"} {
		if uploadErrorMatchesProvider("cos", "reset", message) {
			test.Fatalf("unrelated failure accepted: %s", message)
		}
	}
	if uploadErrorMatchesProvider("cos", "quota", strings.ReplaceAll(samples["quota"], "507 InsufficientStorage", "403 AccessDenied")) {
		test.Fatal("permission error counted as capacity fault")
	}
}

func TestCOSCompletionFaultLogsRequireExactOperationAndRecorder(test *testing.T) {
	for _, app := range []string{"flv", "ts"} {
		message := "close failed,sync=true,block=true: Post \"http://127.0.0.1:9000/" + app + "%2Fsample%2F1." + app + "?redacted\": context deadline exceeded"
		var log strings.Builder
		encoder := json.NewEncoder(&log)
		if err := encoder.Encode(map[string]string{"level": "ERROR", "msg": message}); err != nil {
			test.Fatal(err)
		}
		if _, err := finalizationErrorsForProvider([]byte(log.String()), app, "cos"); err == nil {
			test.Fatal("missing recorder diagnostic accepted")
		}
		if err := encoder.Encode(map[string]string{"level": "ERROR", "msg": "[" + app + "/sample]recording configuration finalization: " + message}); err != nil {
			test.Fatal(err)
		}
		if _, err := finalizationErrorsForProvider([]byte(log.String()), app, "cos"); err != nil {
			test.Fatal(err)
		}
		if _, err := finalizationErrorsForProvider([]byte(log.String()), app, "aws"); err == nil {
			test.Fatal("COS completion accepted as S3")
		}
		for _, invalid := range []string{strings.ReplaceAll(log.String(), "Post ", "Put "), strings.ReplaceAll(log.String(), "context deadline exceeded", "AccessDenied"), log.String() + log.String()} {
			if _, err := finalizationErrorsForProvider([]byte(invalid), app, "cos"); err == nil {
				test.Fatal("wrong operation, cause or count accepted")
			}
		}
	}
}
