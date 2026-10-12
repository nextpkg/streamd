package hls

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/hls/item"
	"github.com/nextpkg/streamd/rtmp/core"
)

type activityResponse struct {
	*httptest.ResponseRecorder
	failure error
	short   bool
	hook    func()
}

func (response *activityResponse) Write(body []byte) (int, error) {
	if response.hook != nil {
		response.hook()
	}
	if response.failure != nil {
		return 0, response.failure
	}
	if response.short {
		return max(0, len(body)-1), nil
	}
	return response.ResponseRecorder.Write(body)
}

func activityStream(test *testing.T, signed bool) (*Server, *Stream, *http.ServeMux) {
	app := strings.ReplaceAll(test.Name(), "/", "-")
	settings := config.AppHLS{Enable: true, Fragment: 4, PlaylistLength: 2}
	if signed {
		settings.SecureKey, settings.SecureTTL = "viewer-test-key", 60
	}
	config.Get().SetApp(app, &config.App{Enable: true, AppName: app, HLS: settings})
	server := &Server{}
	writer, err := server.GetWriter(base.NewInfo(app, "sample", false), nil)
	if err != nil {
		test.Fatal(err)
	}
	stream := writer.(*core.Funnel).Terminal.(*Stream)
	for index := 1; index <= 2; index++ {
		name := fmt.Sprintf("sample/%d.ts", index)
		if signed {
			name += "?token=issued&e=" + strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10)
		}
		stream.m3u8.SetItem(strconv.Itoa(index), item.NewTsItem(name, 4000, int64(index), []byte{0x47, 1, 2, 3}))
	}
	stream.m3u8.GenerateM3u8()
	test.Cleanup(server.Close)
	return server, stream, server.Init(http.NewServeMux())
}

func TestHLSViewerActivityRequiresSuccessfulGET(test *testing.T) {
	for _, sample := range []struct {
		name, method, route                                         string
		code                                                        int
		active, preparing, disabled, ended, canceled, broken, short bool
	}{
		{name: "playlist", method: http.MethodGet, route: "playlist", code: 200, active: true},
		{name: "segment", method: http.MethodGet, route: "segment", code: 200, active: true},
		{name: "head-playlist", method: http.MethodHead, route: "playlist", code: 200},
		{name: "head-segment", method: http.MethodHead, route: "segment", code: 200},
		{name: "wrong-method", method: http.MethodPost, route: "playlist", code: 405},
		{name: "missing", method: http.MethodGet, route: "missing", code: 404},
		{name: "forged", method: http.MethodGet, route: "forged", code: 403},
		{name: "expired", method: http.MethodGet, route: "expired", code: 410},
		{name: "preparing", method: http.MethodGet, route: "playlist", code: 202, preparing: true},
		{name: "disabled", method: http.MethodGet, route: "playlist", code: 200, disabled: true},
		{name: "ended", method: http.MethodGet, route: "playlist", code: 200, ended: true},
		{name: "canceled", method: http.MethodGet, route: "segment", code: 200, canceled: true},
		{name: "broken-segment", method: http.MethodGet, route: "segment", code: 200, broken: true},
		{name: "short-playlist", method: http.MethodGet, route: "playlist", code: 200, short: true},
	} {
		test.Run(sample.name, func(test *testing.T) {
			_, stream, mux := activityStream(test, true)
			if sample.preparing {
				stream.m3u8.Cleanup()
			}
			if sample.disabled {
				stream.setActive(false)
			}
			if sample.ended {
				stream.m3u8.MarkEnd()
			}
			location := "/hls/" + stream.Info().App + "/sample.m3u8"
			if sample.route != "playlist" {
				expiration := time.Now().Add(time.Minute).Unix()
				if sample.route == "expired" {
					expiration = time.Now().Add(-time.Second).Unix()
				}
				token := config.AntiLeechSign(stream.Info().Key, "viewer-test-key", expiration)
				if sample.route == "forged" {
					token = "invalid"
				}
				sequence := "1"
				if sample.route == "missing" {
					sequence = "99"
				}
				location = "/hls/" + stream.Info().App + "/sample/" + sequence + ".ts?token=" + token + "&e=" + strconv.FormatInt(expiration, 10)
			}
			request := httptest.NewRequest(sample.method, location, nil)
			if sample.canceled {
				ctx, cancel := context.WithCancel(request.Context())
				cancel()
				request = request.WithContext(ctx)
			}
			response := &activityResponse{ResponseRecorder: httptest.NewRecorder(), short: sample.short}
			if sample.broken {
				response.failure = syscall.EPIPE
			}
			mux.ServeHTTP(response, request)
			if response.Code != sample.code {
				test.Fatalf("HTTP %d expected %d", response.Code, sample.code)
			}
			if stream.ViewerDeadline().After(time.Now()) != sample.active {
				test.Fatalf("viewer activity=%v expected %v", stream.ViewerDeadline(), sample.active)
			}
		})
	}
}

func TestHLSViewerWindowResizeDisableAndClose(test *testing.T) {
	previous := config.Get().RTMP.EdgeAlive
	config.Get().RTMP.EdgeAlive = 3
	defer func() { config.Get().RTMP.EdgeAlive = previous }()
	_, stream, _ := activityStream(test, false)
	now := time.Now()
	stream.refreshViewer(now, stream.m3u8.Duration())
	if !stream.ViewerDeadline().Equal(now.Add(8 * time.Second)) {
		test.Fatalf("playlist window not respected: %v", stream.ViewerDeadline().Sub(now))
	}
	stream.m3u8.Resize(1)
	stream.m3u8.GenerateM3u8()
	stream.refreshViewer(now.Add(time.Second), stream.m3u8.Duration())
	if !stream.ViewerDeadline().Equal(now.Add(8 * time.Second)) {
		test.Fatal("resize revoked a previously granted viewing window")
	}
	stream.refreshViewer(now.Add(10*time.Second), stream.m3u8.Duration())
	if !stream.ViewerDeadline().Equal(now.Add(14 * time.Second)) {
		test.Fatal("new requests did not use the smaller bounded window")
	}
	stream.setActive(false)
	stream.refreshViewer(now.Add(11*time.Second), stream.m3u8.Duration())
	stream.setActive(true)
	if !stream.ViewerDeadline().IsZero() {
		test.Fatal("disable/enable revived an old viewer")
	}
	stream.refreshViewer(now.Add(12*time.Second), stream.m3u8.Duration())
	stream.Close()
	if !stream.active.Load() {
		test.Fatal("shutdown disabled packet draining before the funnel ended")
	}
	stream.refreshViewer(now.Add(13*time.Second), stream.m3u8.Duration())
	if !stream.ViewerDeadline().IsZero() {
		test.Fatal("closed HLS worker retained viewer activity")
	}
}

func TestHLSViewerKeepsTheWindowActuallyIssued(test *testing.T) {
	previous := config.Get().RTMP.EdgeAlive
	config.Get().RTMP.EdgeAlive = 3
	defer func() { config.Get().RTMP.EdgeAlive = previous }()
	_, stream, mux := activityStream(test, false)
	response := &activityResponse{ResponseRecorder: httptest.NewRecorder(), hook: func() { stream.m3u8.Resize(1); stream.m3u8.GenerateM3u8() }}
	started := time.Now()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/hls/"+stream.Info().App+"/sample.m3u8", nil))
	if strings.Count(response.Body.String(), "#EXTINF:") != 2 {
		test.Fatal("test did not issue the original playlist")
	}
	if stream.m3u8.Duration() != 4*time.Second {
		test.Fatal("test did not concurrently shrink the cache")
	}
	if stream.ViewerDeadline().Before(started.Add(8 * time.Second)) {
		test.Fatal("in-flight old playlist inherited the smaller new window")
	}
}

func TestHLSViewerActivityConcurrentRequestsAndRetirement(test *testing.T) {
	_, stream, mux := activityStream(test, false)
	location := "/hls/" + stream.Info().App + "/sample.m3u8"
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for request := 0; request < 30; request++ {
				mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, location, nil))
				_ = stream.ViewerDeadline()
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for change := 0; change < 30; change++ {
			stream.setActive(false)
			stream.m3u8.MarkEnd()
			stream.m3u8.MarkLive(false)
			stream.setActive(true)
		}
	}()
	workers.Wait()
	stream.Close()
	stream.refreshViewer(time.Now(), stream.m3u8.Duration())
	if !stream.ViewerDeadline().IsZero() {
		test.Fatal("activity survived retirement")
	}
}
