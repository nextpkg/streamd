package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitPlaylistRejectsRetiredGeneration(test *testing.T) {
	const live = "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.000,\nsample/1.ts\n"
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch requests.Add(1) {
		case 1:
			_, _ = fmt.Fprint(response, live+"#EXT-X-ENDLIST\n") // The HTTP client validates the response; disconnects need no retry.
		case 2:
			http.Error(response, "playlist is preparing", http.StatusAccepted)
		default:
			_, _ = fmt.Fprint(response, live) // The HTTP client validates the response; disconnects need no retry.
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	harness := &harness{ctx: ctx, origin: server.URL, httpClient: server.Client()}
	playlist, err := harness.waitPlaylist("dynamic")
	if err != nil || requests.Load() != 3 || strings.Contains(playlist, "#EXT-X-ENDLIST") {
		test.Fatalf("retired generation accepted as live: requests=%d playlist=%q err=%v", requests.Load(), playlist, err)
	}
}

func TestWaitRelayPlaylistStoppedRequiresRetirement(test *testing.T) {
	for _, gone := range []bool{false, true} {
		test.Run(fmt.Sprintf("removed=%v", gone), func(test *testing.T) {
			var requests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if requests.Add(1) == 1 {
					_, _ = fmt.Fprint(response, "#EXTM3U\n#EXTINF:2.000,\nsample/1.ts\n") // The HTTP client validates the response; disconnects need no retry.
					return
				}
				if gone {
					http.NotFound(response, request)
					return
				}
				_, _ = fmt.Fprint(response, "#EXTM3U\n#EXTINF:2.000,\nsample/1.ts\n#EXT-X-ENDLIST\n") // The HTTP client validates the response; disconnects need no retry.
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			harness := &harness{ctx: ctx, origin: server.URL, httpClient: server.Client()}
			if err := harness.waitRelayPlaylistStopped("dynamic"); err != nil || requests.Load() != 2 {
				test.Fatalf("live playlist accepted as stopped: requests=%d err=%v", requests.Load(), err)
			}
		})
	}
}
