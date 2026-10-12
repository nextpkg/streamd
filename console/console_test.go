package console

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRelayURLValidationAndNormalization(test *testing.T) {
	server := &Server{}
	if err := server.checkRtmpURL(nil); err == nil {
		test.Fatal("nil URL accepted")
	}
	for _, invalid := range []string{"", "http://127.0.0.1/a/b", "rtmp://127.0.0.1/a/", "rtmp://127.0.0.1/a", "rtmp://127.0.0.1/a/b/c", "rtmp://127.0.0.1:0/a/b", "rtmp://127.0.0.1:65536/a/b", "rtmp://127.0.0.1:bad/a/b", "rtmp://user:secret@127.0.0.1/a/b", "rtmp://127.0.0.1/a/b#fragment"} {
		location := invalid
		if err := server.checkRtmpURL(&location); err == nil {
			test.Errorf("invalid URL accepted: %q", invalid)
		}
	}
	for input, expected := range map[string]string{
		"127.0.0.1/a/b":          "rtmp://127.0.0.1:1935/a/b",
		"rtmp://127.0.0.1/a/b":   "rtmp://127.0.0.1:1935/a/b",
		"rtmp://[::1]/a/b":       "rtmp://[::1]:1935/a/b",
		"//127.0.0.1:2000/a/b":   "rtmp://127.0.0.1:2000/a/b",
		"rtmp://127.0.0.1/a/b?q": "rtmp://127.0.0.1:1935/a/b?q",
	} {
		location := input
		if err := server.checkRtmpURL(&location); err != nil || location != expected {
			test.Errorf("normalization %q: got %q, err=%v", input, location, err)
		}
	}
}

func TestAdminMethodsRemainRestrictedWithFallback(test *testing.T) {
	router := (&Server{}).Init(http.NewServeMux())
	router.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) { http.NotFound(writer, request) })
	for _, path := range []string{"/admin/stat/keys", "/admin/stat/pub/info", "/admin/stat/sub/info", "/admin/stat/sub/online", "/admin/config", "/admin/relay/start"} {
		method, allowed := http.MethodPost, http.MethodGet
		if path == "/admin/relay/start" {
			method, allowed = http.MethodGet, http.MethodPost
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(method, path, nil))
		if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != allowed {
			test.Fatalf("%s %s: status=%d Allow=%q", method, path, response.Code, response.Header().Get("Allow"))
		}
	}
}
