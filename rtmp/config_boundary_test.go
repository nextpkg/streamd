package rtmp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/rtmp/core"
)

func TestOriginReloadKeepsFlightPolicyAndRefreshesDialOptions(t *testing.T) {
	app := t.Name()
	initial, updated := config.NewConfig(), config.NewConfig()
	initial.SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://127.0.0.1/first", "rtmp://127.0.0.2/second"}})
	updated.SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://127.0.0.3/new"}})
	initial.RTMP.DialTimeout = 3
	updated.RTMP.DialTimeout = 7
	updated.RTMP.BufSize = 8192
	updated.RTMP.SlabMin, updated.RTMP.SlabMax = 2, 64
	var current atomic.Pointer[config.Config]
	current.Store(initial)
	connection := newConn(context.Background(), current.Load)
	defer connection.Close()
	type attempt struct {
		address string
		timeout time.Duration
		options core.ClientOptions
	}
	var attempts []attempt
	connection.pull = func(_ context.Context, _ *base.Info, address string, timeout time.Duration, options core.ClientOptions) (base.ReadCloser, error) {
		attempts = append(attempts, attempt{address, timeout, options})
		// A reload during the first dial must not replace the in-flight candidate list.
		current.Store(updated)
		return nil, errors.New("dial failed")
	}
	for i := 0; i < 2; i++ {
		writer := core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(base.NewInfo(app, "sample", true))})
		connection.InstallWriter(writer)
		closed := writer.IsClosed()
		writer.Close()
		writer.Wait()
		if !closed {
			t.Fatal("failed origin retained viewer")
		}
	}
	expected := []attempt{
		{"rtmp://127.0.0.1/first/sample", 3 * time.Second, core.ClientOptions{BufferSize: initial.RTMP.BufSize}},
		{"rtmp://127.0.0.2/second/sample", 3 * time.Second, core.ClientOptions{BufferSize: 8192, SlabMin: 2048, SlabMax: 65536}},
		{"rtmp://127.0.0.3/new/sample", 7 * time.Second, core.ClientOptions{BufferSize: 8192, SlabMin: 2048, SlabMax: 65536}},
	}
	if len(attempts) != len(expected) {
		t.Fatalf("attempts = %+v", attempts)
	}
	for i := range expected {
		if attempts[i] != expected[i] {
			t.Errorf("attempt %d = %+v, want %+v", i, attempts[i], expected[i])
		}
	}
}
