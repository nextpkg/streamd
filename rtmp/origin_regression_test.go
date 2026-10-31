package rtmp

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/rtmp/core"
)

func TestConcurrentColdViewersWaitForOriginStartup(test *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			accepted <- connection
		}
	}()
	app := test.Name()
	config.Get().SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://" + listener.Addr().String() + "/source"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := NewConn(ctx)
	defer connection.Close()
	var callers sync.WaitGroup
	writers := make([]*core.Funnel, 13)
	for index := range writers {
		writers[index] = core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(base.NewInfo(app, "sample", true))})
		defer writers[index].Close()
	}
	callers.Add(1)
	go func() { defer callers.Done(); connection.InstallWriter(writers[0]) }()
	var origin net.Conn
	select {
	case origin = <-accepted:
	case <-time.After(time.Second):
		test.Fatal("origin startup was not attempted")
	}
	defer origin.Close()
	started := make(chan struct{}, len(writers)-1)
	for _, writer := range writers[1:] {
		callers.Add(1)
		go func() {
			defer callers.Done()
			started <- struct{}{}
			connection.InstallWriter(writer)
		}()
	}
	for range writers[1:] {
		<-started
	}
	time.Sleep(50 * time.Millisecond)
	for index, writer := range writers {
		if writer.IsClosed() {
			test.Errorf("cold viewer %d was rejected while origin startup was still in progress", index)
		}
	}
	_ = origin.Close()
	finished := make(chan struct{})
	go func() { callers.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		test.Fatal("origin failure did not release waiting viewers")
	}
	for _, writer := range writers {
		if !writer.IsClosed() {
			test.Error("failed origin left a waiting viewer open")
		}
		writer.Wait()
	}
}

func TestOriginSingleFlightRetainsWarmStreamAndExternalIdentity(test *testing.T) {
	app := test.Name()
	config.Get().SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://127.0.0.1/source"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := NewConn(ctx)
	defer connection.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	reader := &takeoverReader{Universal: core.NewUniversal(base.NewInfo(app, "sample", true)), done: make(chan struct{})}
	connection.pull = func(ctx context.Context, _ *base.Info, _ string, _ time.Duration, _ core.ClientOptions) (base.ReadCloser, error) {
		if attempts.Add(1) != 1 {
			return nil, errors.New("duplicate origin connection")
		}
		close(entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return reader, nil
		}
	}
	writers := make([]*core.Funnel, 16)
	var workers sync.WaitGroup
	for index := range writers {
		writer := core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(base.NewInfo(app, "sample", true))})
		writers[index] = writer
		defer func() { writer.Close(); writer.Wait() }()
		workers.Add(1)
		go func() { defer workers.Done(); connection.InstallWriter(writer) }()
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		test.Fatal("origin never started")
	}
	close(release)
	workers.Wait()
	value, exists := connection.stream.Load(reader.Info().Key)
	if !exists || !value.(*Stream).info.IsExternal || len(connection.PublicationKeys()) != 0 {
		test.Fatal("origin was misclassified as a locally registered publisher")
	}
	for _, writer := range writers {
		if writer.IsClosed() {
			test.Fatal("concurrent cold viewer was rejected")
		}
	}
	if count, _ := connection.WriterActive(reader.Info().Key); count != int64(len(writers)) || attempts.Load() != 1 {
		test.Fatalf("origin was not shared: viewers=%d attempts=%d", count, attempts.Load())
	}
	config.Get().SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://127.0.0.2/replacement"}})
	warm := core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(base.NewInfo(app, "sample", true))})
	defer func() { warm.Close(); warm.Wait() }()
	connection.InstallWriter(warm)
	current, _ := connection.stream.Load(reader.Info().Key)
	if current != value || attempts.Load() != 1 || warm.IsClosed() {
		test.Fatal("source policy update replaced a healthy cached origin")
	}
	connection.Close()
	select {
	case <-reader.done:
	default:
		test.Fatal("shutdown leaked origin reader")
	}
}

func TestOriginCacheReuseExpiryPolicyAndFailureCooldown(test *testing.T) {
	app := test.Name()
	cfg := config.Get()
	cfg.SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://127.0.0.1/source"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := NewConn(ctx)
	defer connection.Close()
	info := base.NewInfo(app, "sample", true)
	first := connection.originSources(info, cfg)
	if first == nil || connection.originSources(info, cfg) != first {
		test.Fatal("warm source-list cache was not reused")
	}
	first.time = time.Now().Unix() - int64(cfg.Finder.Expire) - 1
	second := connection.originSources(info, cfg)
	if second == first {
		test.Fatal("expired source-list cache was reused")
	}
	cfg.SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://127.0.0.2/replacement"}})
	third := connection.originSources(info, cfg)
	if third == second || third.data[0].pullHost != "rtmp://127.0.0.2/replacement/sample" {
		test.Fatal("new source policy did not replace the cache for a new origin")
	}
	var attempts atomic.Int32
	connection.pull = func(context.Context, *base.Info, string, time.Duration, core.ClientOptions) (base.ReadCloser, error) {
		attempts.Add(1)
		return nil, errors.New("injected origin failure")
	}
	for repeat := 0; repeat < 3; repeat++ {
		writer := core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(info.Copy())})
		connection.InstallWriter(writer)
		writer.Close()
		writer.Wait()
	}
	if attempts.Load() != 1 {
		test.Fatal("failed origin cooldown was removed")
	}
}

func TestOriginStartupCannotReplaceNewLocalPublisher(test *testing.T) {
	app := test.Name()
	config.Get().SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://127.0.0.1/source"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := NewConn(ctx)
	defer connection.Close()
	entered, release, installed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	origin := &takeoverReader{Universal: core.NewUniversal(base.NewInfo(app, "sample", true)), done: make(chan struct{})}
	connection.pull = func(ctx context.Context, _ *base.Info, _ string, _ time.Duration, _ core.ClientOptions) (base.ReadCloser, error) {
		close(entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return origin, nil
		}
	}
	writer := core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(base.NewInfo(app, "sample", true))})
	defer func() { writer.Close(); writer.Wait() }()
	go func() { connection.InstallWriter(writer); close(installed) }()
	<-entered
	local := &takeoverReader{Universal: core.NewUniversal(base.NewInfo(app, "sample", false)), done: make(chan struct{})}
	connection.InstallReader(local)
	close(release)
	select {
	case <-installed:
	case <-time.After(time.Second):
		test.Fatal("origin admission hung")
	}
	value, _ := connection.stream.Load(local.Info().Key)
	if value.(*Stream).Reader().ReadCloser != local || len(connection.PublicationKeys()) != 1 || writer.IsClosed() {
		test.Fatal("late origin replaced a local publisher or lost its viewer")
	}
	select {
	case <-origin.done:
	default:
		test.Fatal("unused origin connection leaked")
	}
}

func TestOriginShutdownCancelsBlockedStartup(test *testing.T) {
	app := test.Name()
	config.Get().SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://127.0.0.1/source"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := NewConn(ctx)
	entered, installed := make(chan struct{}), make(chan struct{})
	connection.pull = func(ctx context.Context, _ *base.Info, _ string, _ time.Duration, _ core.ClientOptions) (base.ReadCloser, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	writer := core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(base.NewInfo(app, "sample", true))})
	defer func() { writer.Close(); writer.Wait() }()
	go func() { connection.InstallWriter(writer); close(installed) }()
	<-entered
	closed := make(chan struct{})
	go func() { connection.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		test.Fatal("shutdown did not cancel origin startup")
	}
	select {
	case <-installed:
	case <-time.After(time.Second):
		test.Fatal("shutdown left the viewer waiting")
	}
	if !writer.IsClosed() || len(connection.StreamKeys()) != 0 {
		test.Fatal("shutdown leaked origin state")
	}
}

func TestOriginStartupRejectsApplicationRemovedWhileConnecting(test *testing.T) {
	app := test.Name()
	initial := config.NewConfig()
	initial.SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://127.0.0.1/source"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var current atomic.Pointer[config.Config]
	current.Store(initial)
	connection := newConn(ctx, current.Load)
	defer connection.Close()
	reader := &takeoverReader{Universal: core.NewUniversal(base.NewInfo(app, "sample", true)), done: make(chan struct{})}
	pulled := false
	connection.pull = func(context.Context, *base.Info, string, time.Duration, core.ClientOptions) (base.ReadCloser, error) {
		pulled = true
		current.Store(config.NewConfig())
		return reader, nil
	}
	writer := core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(base.NewInfo(app, "sample", true))})
	defer func() { writer.Close(); writer.Wait() }()
	connection.InstallWriter(writer)
	if !pulled {
		test.Fatal("origin did not use the injected application configuration")
	}
	if !writer.IsClosed() || len(connection.StreamKeys()) != 0 {
		test.Fatal("removed application admitted a new origin")
	}
	select {
	case <-reader.done:
	default:
		test.Fatal("removed application's source connection leaked")
	}
}

func TestFailedOriginDoesNotLeavePlaceholderStream(test *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	app := test.Name()
	config.Get().SetApp(app, &config.App{Enable: true, AppName: app, StaticPull: []string{"rtmp://" + address + "/source"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := NewConn(ctx)
	defer connection.Close()
	writer := core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(base.NewInfo(app, "sample", true))})
	defer func() { writer.Close(); writer.Wait() }()
	connection.InstallWriter(writer)
	if !writer.IsClosed() || len(connection.StreamKeys()) != 0 {
		test.Fatalf("failed origin poisoned later viewers: closed=%v streams=%v", writer.IsClosed(), connection.StreamKeys())
	}
}
