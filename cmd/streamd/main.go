package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/console"
	"github.com/nextpkg/streamd/hfs"
	"github.com/nextpkg/streamd/hls"
	"github.com/nextpkg/streamd/internal/httpx"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/metrics"
	"github.com/nextpkg/streamd/proxy"
	"github.com/nextpkg/streamd/record"
	"github.com/nextpkg/streamd/rtmp"
)

var version = "development"

func main() {
	filename := flag.String("config", "streamd.yaml", "configuration file")
	check := flag.Bool("check-config", false, "validate configuration without opening listeners")
	showVersion := flag.Bool("version", false, "print version")
	slabMin := flag.Int("slab-min", 0, "override slab minimum threshold in KiB; zero uses configuration")
	slabMax := flag.Int("slab-max", 0, "override slab allocation size in KiB; zero uses configuration")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if err := config.SetSlabOverrides(*slabMin, *slabMax); err != nil {
		log.Error(err)
		os.Exit(1)
	}
	if *check {
		data, err := os.ReadFile(*filename)
		if err == nil {
			_, err = config.Decode(data)
		}
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
		fmt.Println("configuration valid")
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *filename); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, filename string) error {
	defer httpx.CloseCallbacks()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	manager, err := config.NewManager(ctx, filename)
	if err != nil {
		return err
	}
	defer func() { config.Get().Revoke() }()
	hlsServer := &hls.Server{}
	recorder := record.NewServer()
	streams := rtmp.NewConn(ctx)
	admin := console.NewServer(streams)
	router := proxy.Init(admin, hfs.NewServer(streams), hlsServer)
	defer router.Close()
	router.Handle("GET /metrics", metrics.Handler())
	router.HandleFunc("GET /healthz", func(response http.ResponseWriter, request *http.Request) { _, _ = response.Write([]byte("ok\n")) })
	router.HandleFunc("GET /readyz", func(response http.ResponseWriter, request *http.Request) {
		_, _ = fmt.Fprintf(response, "ready configuration=%d\n", manager.Version())
	})
	server := &http.Server{Addr: config.Get().HTTP.Addr, Handler: router, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	listener, err := streams.StartRTMP(hlsServer, recorder)
	if err != nil {
		return err
	}
	manager.OnCommit = func(previous, next *config.Config) {
		for _, key := range streams.PublicationKeys() {
			app, instance, valid := strings.Cut(key, "/")
			if valid {
				next.Finder.Register(app, instance)
			}
		}
	}
	failures := make(chan error, 2)
	go func() { failures <- server.ListenAndServe() }()
	go func() { failures <- manager.Watch(ctx) }()
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	defer signal.Stop(reload)
	log.Infof("HTTP listening on %s; configuration version %d", server.Addr, manager.Version())
running:
	for {
		select {
		case <-ctx.Done():
			break running
		case failure := <-failures:
			if failure != nil && !errors.Is(failure, http.ErrServerClosed) {
				err = failure
			}
			break running
		case <-reload:
			if failure := manager.Reload(); failure != nil {
				log.Error("configuration reload rejected: ", failure)
			}
		}
	}
	_ = listener.Close()
	router.Close()
	streams.Close()
	admin.Close()
	hlsServer.Close()
	recorder.Close()
	httpx.CloseCallbacks()
	cancel()
	shutdown, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	if failure := server.Shutdown(shutdown); failure != nil {
		_ = server.Close()
		err = errors.Join(err, failure)
	}
	return err
}
