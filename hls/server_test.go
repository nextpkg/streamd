package hls

import (
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
)

func TestServerShutdownClosesFunnelAndRetention(t *testing.T) {
	config.Get().SetApp("shutdown", &config.App{Enable: true, AppName: "shutdown", HLS: config.AppHLS{Enable: true, Fragment: 2, PlaylistLength: 6}})
	server := &Server{}
	writer, err := server.GetWriter(base.NewInfo("shutdown", "sample", false), nil)
	if err != nil || writer == nil {
		t.Fatalf("create HLS writer: %v", err)
	}
	finished := make(chan struct{})
	go func() { server.Close(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("HLS shutdown stalled")
	}
	if server.getConn("shutdown/sample") != nil {
		t.Fatal("closed HLS stream retained")
	}
	if _, err := server.GetWriter(base.NewInfo("shutdown", "late", false), nil); err == nil {
		t.Fatal("new stream accepted after shutdown")
	}
}
