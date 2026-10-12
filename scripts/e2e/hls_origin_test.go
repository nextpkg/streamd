package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHLSObservationPreservesIssuedWindow(test *testing.T) {
	const longPlaylist = "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\na.ts\n#EXTINF:4,\nb.ts\n"
	const shortPlaylist = "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\na.ts\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/long.m3u8":
			_, _ = io.WriteString(writer, longPlaylist)
		case "/short.m3u8":
			_, _ = io.WriteString(writer, shortPlaylist)
		case "/a.ts":
			_, _ = io.WriteString(writer, "segment bytes")
		default:
			http.NotFound(writer, request)
		}
	}))
	defer upstream.Close()
	observation, err := observeHLS(upstream.URL, 3*time.Second)
	if err != nil {
		test.Fatal(err)
	}
	defer observation.server.Close()
	fetch := func(method, location, expected string) {
		test.Helper()
		request, err := http.NewRequest(method, observation.server.URL+location, nil)
		if err != nil {
			test.Fatal(err)
		}
		response, err := observation.server.Client().Do(request)
		if err != nil {
			test.Fatal(err)
		}
		body, readError := io.ReadAll(response.Body)
		closeError := response.Body.Close()
		if readError != nil || closeError != nil || string(body) != expected {
			test.Fatalf("response changed: %q read=%v close=%v", body, readError, closeError)
		}
	}
	fetch(http.MethodGet, "/long.m3u8", longPlaylist)
	deadline := observation.deadline.Load()
	if time.Duration(deadline-observation.lastFetch.Load()) != 8*time.Second {
		test.Fatal("did not derive the response's actual eight-second window")
	}
	fetch(http.MethodGet, "/short.m3u8", shortPlaylist)
	fetch(http.MethodGet, "/a.ts", "segment bytes")
	lastFetch := observation.lastFetch.Load()
	if observation.deadline.Load() != deadline || observation.window.Load() != int64(2*time.Second) {
		test.Fatal("a shorter playlist revoked previously issued grace")
	}
	fetch(http.MethodHead, "/long.m3u8", "")
	fetch(http.MethodGet, "/missing.m3u8", "404 page not found\n")
	if observation.lastFetch.Load() != lastFetch || observation.playlistRequests.Load() != 2 || observation.segmentRequests.Load() != 1 {
		test.Fatal("HEAD or unsuccessful requests changed activity")
	}
}

func TestHLSOriginLogValidationRequiresOneSession(test *testing.T) {
	line := func(level, message string) string {
		body, err := json.Marshal(map[string]string{"level": level, "msg": message})
		if err != nil {
			test.Fatal(err)
		}
		return string(body) + "\n"
	}
	play := line("INFO", "[origin/sample]New PLAY from 127.0.0.1:1")
	origin := line("INFO", "[origin/sample]Origin attached from rtmp://127.0.0.1/origin/sample")
	idle := line("INFO", "[origin/sample]Data source idling over 3s.")
	source := line("INFO", "[origin/sample]New PUBLISH from 127.0.0.1:2") + play
	edge := play + origin + idle
	for _, sample := range []struct {
		body        string
		edge, valid bool
	}{
		{source, false, true}, {edge, true, true}, {edge + origin, true, false}, {strings.TrimSuffix(edge, idle), true, false},
		{source + play, false, false}, {edge + line("ERROR", "recording failed"), true, false}, {source + "panic: failed\n", false, false},
	} {
		if _, err := validateHLSOriginLog(sample.body, sample.edge); (err == nil) != sample.valid {
			test.Errorf("HLS origin log valid=%v got %v", sample.valid, err)
		}
	}
}

func TestHLSPlaylistWindowRejectsInvalidManifest(test *testing.T) {
	for _, sample := range []struct {
		body   string
		window time.Duration
	}{
		{"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4.000,\na.ts\n#EXTINF:2.000,\nb.ts\n", 8 * time.Second},
		{"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:1.920,\na.ts\n", 2 * time.Second},
		{"#EXTM3U\n", 0}, {"#EXT-X-TARGETDURATION:4\n", 0}, {"#EXT-X-TARGETDURATION:0\n#EXTINF:2,\na.ts\n", 0},
		{"#EXT-X-TARGETDURATION:2.5\n#EXTINF:2,\na.ts\n", 0}, {"#EXT-X-TARGETDURATION:9223372036854775807\n#EXTINF:2,\na.ts\n", 0},
		{"#EXT-X-TARGETDURATION:2\n#EXT-X-TARGETDURATION:3\n#EXTINF:2,\na.ts\n", 0},
	} {
		window, err := hlsPlaylistWindow(sample.body)
		if window != sample.window || (err == nil) != (sample.window > 0) {
			test.Errorf("window=%v err=%v for %q", window, err, sample.body)
		}
	}
}
