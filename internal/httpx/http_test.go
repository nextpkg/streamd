package httpx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func postCallbackTest(address string, data []byte) error {
	executor := newCallbackExecutor()
	defer executor.close()
	return executor.post(address, data)
}

func TestCallbackRejectsTruncatedSuccessfulResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Length", "20")
		_, _ = response.Write([]byte("short"))
	}))
	defer server.Close()
	if err := postCallbackTest(server.URL, []byte(`{"event":"test"}`)); err == nil {
		t.Fatal("truncated HTTP 200 body was accepted as a complete callback")
	}
}

func TestCallbackRejectsOversizedSuccessfulResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = response.Write([]byte(strings.Repeat("x", 64*1024+1)))
	}))
	defer server.Close()
	if err := postCallbackTest(server.URL, []byte(`{"event":"test"}`)); err == nil {
		t.Fatal("oversized HTTP 200 body was accepted as a complete callback")
	}
}

func TestCallbackFailureDoesNotExposeEndpointSecrets(t *testing.T) {
	err := postCallbackTest("http://private-user:private-password@127.0.0.1:0/callback?token=private-token", []byte(`{}`))
	if err == nil {
		t.Fatal("invalid destination unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "private-") || strings.Contains(err.Error(), "token=") {
		t.Fatalf("callback diagnostics exposed endpoint contents: %v", err)
	}
}
