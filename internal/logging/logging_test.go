package logging

import (
	"strings"
	"testing"
)

func TestRedactCredentials(test *testing.T) {
	message := "request rtmp://alice:password-value@example.test/live/stream?token=query-secret failed token=token-value SecretKey=secret-value"
	redacted := redact(message)
	for _, secret := range []string{"alice", "password-value", "query-secret", "token-value", "secret-value"} {
		if strings.Contains(redacted, secret) {
			test.Fatalf("sensitive value remained after redaction")
		}
	}
	if !strings.Contains(redacted, "example.test/live/stream") {
		test.Fatal("redaction removed non-sensitive routing context")
	}
	for _, sample := range []struct {
		message string
		secret  string
	}{
		{`Authorization: Bearer bearer-value`, "bearer-value"},
		{`Authorization=Basic encoded-value`, "encoded-value"},
		{`{"SecretKey":"quoted secret"}`, "quoted secret"},
		{`{"api_key":"escaped\"value"}`, "value"},
	} {
		if strings.Contains(redact(sample.message), sample.secret) {
			test.Fatal("structured credential remained after redaction")
		}
	}
}
