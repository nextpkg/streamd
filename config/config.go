package config

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

type Config struct {
	Revision uint64   `json:"-"`
	HTTP     HTTP     `json:"http"`
	LogLevel string   `json:"log_level"`
	Console  Console  `json:"console"`
	Metadata Metadata `json:"metadata"`
	Finder   Finder   `json:"finder"`
	RTMP     RTMP     `json:"rtmp"`
	App      []App    `json:"application"`
	Metrics  Metrics  `json:"metrics"`
	Proxy    Proxy    `json:"proxy"`
	app      map[string]*App
	appMu    sync.RWMutex
}

type HTTP struct {
	Addr string `json:"addr"`
}
type Console struct {
	EnableRelay bool `json:"enable_relay"`
}
type Metadata struct {
	Provider string `json:"provider"`
	Service  string `json:"service"`
}
type Metrics struct {
	Cluster string `json:"cluster"`
}

var current atomic.Pointer[Config]

func init() {
	candidate := NewConfig()
	if err := candidate.Validate(); err != nil {
		panic(err)
	}
	if err := candidate.InitFinder(context.Background()); err != nil {
		panic(err)
	}
	current.Store(candidate)
}

func Get() *Config { return current.Load() }

func NewConfig() *Config {
	return &Config{
		HTTP: HTTP{Addr: "127.0.0.1:8080"}, LogLevel: "INFO",
		Metadata: Metadata{Provider: "streamd", Service: "Streaming media service"},
		Finder:   Finder{Expire: 60, Interval: 30, EnableRelay: true, Separator: "/"},
		Proxy:    Proxy{Prefix: "live"},
		RTMP:     RTMP{Addr: "0.0.0.0:1935", Gop: 1, ChunkSize: 1024, BufSize: 4096, EdgeAlive: 30, EdgeCheck: 10, ReadTimeout: 10, DialTimeout: 2},
		App:      []App{{Enable: true, AppName: "live"}},
	}
}

func (cfg *Config) Validate() error {
	if err := cfg.RTMP.validate(); err != nil {
		return err
	}
	if cfg.HTTP.Addr != "" {
		if _, _, err := net.SplitHostPort(cfg.HTTP.Addr); err != nil {
			return errors.New("invalid HTTP listener address")
		}
	}
	if err := cfg.Finder.check(); err != nil {
		return err
	}
	if err := cfg.Proxy.validate(); err != nil {
		return err
	}
	cfg.app = make(map[string]*App)
	for index := range cfg.App {
		app := &cfg.App[index]
		if !app.Enable {
			continue
		}
		if _, duplicate := cfg.app[app.AppName]; duplicate {
			return fmt.Errorf("duplicate application %q", app.AppName)
		}
		if err := app.validate(); err != nil {
			return fmt.Errorf("validate application: %w", err)
		}
		cfg.app[app.AppName] = app
	}
	return nil
}

func (cfg *Config) InitFinder(ctx context.Context) error { return cfg.Finder.validate(ctx, cfg.app) }
func (cfg *Config) Revoke() {
	cfg.Finder.Close()
	for index := range cfg.App {
		cfg.App[index].Recorder.closeClients()
	}
}

func (cfg *Config) GetApp(name string) *App {
	cfg.appMu.RLock()
	defer cfg.appMu.RUnlock()
	return cfg.app[name]
}

func (cfg *Config) SetApp(name string, app *App) {
	cfg.appMu.Lock()
	defer cfg.appMu.Unlock()
	if cfg.app == nil {
		cfg.app = make(map[string]*App)
	}
	cfg.app[name] = app
}

func (cfg *Config) GetRecorder(name string) *AppRecorder {
	app := cfg.GetApp(name)
	if app == nil || !app.Recorder.Enable {
		return nil
	}
	return &app.Recorder
}

func (cfg *Config) GetHLS(name string) *AppHLS {
	app := cfg.GetApp(name)
	if app == nil || !app.HLS.Enable {
		return nil
	}
	return &app.HLS
}

func (cfg *Config) GetStaticPush(name string) []string {
	if app := cfg.GetApp(name); app != nil {
		return app.StaticPush
	}
	return nil
}

func (cfg *Config) GetStaticPull(name string) []string {
	if app := cfg.GetApp(name); app != nil {
		return app.StaticPull
	}
	return nil
}
