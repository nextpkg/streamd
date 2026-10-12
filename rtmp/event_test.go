package rtmp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPublicationCallbackPayload(t *testing.T) {
	for _, name := range []string{"OnPublish", "OnPublishDone"} {
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
			fireEvent(name, server.URL, "app/stream")()
			select {
			case payload := <-observed:
				if len(payload) != 3 || payload["event"] != name || payload["key"] != "app/stream" {
					t.Fatalf("publication callback contract changed: %v", payload)
				}
				occur, valid := payload["occur"].(float64)
				if !valid || occur < float64(before) || occur > float64(time.Now().UnixMilli()) {
					t.Fatalf("invalid millisecond event timestamp: %v", payload)
				}
			case <-time.After(time.Second):
				t.Fatal("publication callback not delivered")
			}
		})
	}
}
