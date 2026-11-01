package console

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/nextpkg/streamd/rtmp"
)

func TestStatisticsHTTPContractPreservesLegacyQueries(test *testing.T) {
	connection := rtmp.NewConn(context.Background())
	defer connection.Close()
	admin := NewServer(connection)
	defer admin.Close()
	server := httptest.NewServer(admin.Init(http.NewServeMux()))
	defer server.Close()
	for _, route := range []string{"pub/info", "sub/info", "sub/online"} {
		for _, query := range []struct {
			value  string
			status int
		}{
			{"", 400}, {"?key=", 400}, {"?key=%zz", 400}, {"?key=&key=unknown/sample", 400},
			{"?key=unknown/sample", 200}, {"?key=unknown/sample&key=", 200},
			{"?key=not/a/valid/path", 200}, {"?key=%00", 200},
		} {
			response, err := server.Client().Get(server.URL + "/admin/stat/" + route + query.value)
			if err != nil {
				test.Fatal(err)
			}
			var envelope map[string]any
			err = json.NewDecoder(response.Body).Decode(&envelope)
			_ = response.Body.Close()
			if err != nil || response.StatusCode != query.status || response.Header.Get("Content-Type") != "application/json" || len(envelope) != 3 {
				test.Fatalf("%s%s status=%d envelope=%v error=%v", route, query.value, response.StatusCode, envelope, err)
			}
			expected := map[string]any{"code": float64(400), "message": "missing key", "data": nil}
			if query.status == 200 {
				data := map[string]any{"count": float64(0), "list": nil}
				message := "OK"
				if route == "sub/online" {
					data = map[string]any{"count": float64(0), "checkin": float64(0)}
					message = "with-key"
				}
				expected = map[string]any{"code": float64(0), "message": message, "data": data}
			}
			if !reflect.DeepEqual(envelope, expected) {
				test.Fatalf("%s%s got %v want %v", route, query.value, envelope, expected)
			}
		}
	}
	for _, route := range []string{"/admin/stat/keys", "/admin/stat/pub/info?key=unknown/sample", "/admin/stat/sub/info?key=unknown/sample", "/admin/stat/sub/online?key=unknown/sample", "/admin/config"} {
		for _, method := range []string{http.MethodHead, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
			request, err := http.NewRequest(method, server.URL+route, nil)
			if err != nil {
				test.Fatal(err)
			}
			response, err := server.Client().Do(request)
			if err != nil {
				test.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil {
				test.Fatal(err)
			}
			if method == http.MethodHead {
				if response.StatusCode != 200 || len(body) != 0 {
					test.Fatalf("HEAD %s status=%d bytes=%d", route, response.StatusCode, len(body))
				}
			} else if response.StatusCode != 405 || response.Header.Get("Allow") != "GET" {
				test.Fatalf("%s %s status=%d", method, route, response.StatusCode)
			}
		}
	}
}
