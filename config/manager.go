package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/internal/appstate"
	"github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/metrics"
	"go.yaml.in/yaml/v3"
)

type Manager struct {
	path           string
	ctx            context.Context
	mu             sync.Mutex
	version        atomic.Uint64
	watchReady     chan struct{}
	watchReadyOnce sync.Once
	fingerprint    [32]byte
	OnCommit       func(*Config, *Config)
}

func Decode(data []byte) (*Config, error) {
	var document any
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode configuration: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("configuration must contain exactly one document")
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	candidate := NewConfig()
	jsonDecoder := json.NewDecoder(bytes.NewReader(encoded))
	jsonDecoder.DisallowUnknownFields()
	if err := jsonDecoder.Decode(candidate); err != nil {
		return nil, fmt.Errorf("configuration fields: %w", err)
	}
	if address, ok := os.LookupEnv("STREAMD_HTTP_ADDR"); ok {
		candidate.HTTP.Addr = address
	}
	if address, ok := os.LookupEnv("STREAMD_RTMP_ADDR"); ok {
		candidate.RTMP.Addr = address
	}
	if level, ok := os.LookupEnv("STREAMD_LOG_LEVEL"); ok {
		candidate.LogLevel = level
	}
	if err := expandSecrets(candidate); err != nil {
		return nil, err
	}
	if err := candidate.Validate(); err != nil {
		return nil, err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(candidate.LogLevel)); err != nil {
		return nil, errors.New("invalid log_level")
	}
	return candidate, nil
}

func expandSecrets(candidate *Config) error {
	fields := []*string{&candidate.Proxy.SecureKey}
	if candidate.Finder.Redis != nil {
		fields = append(fields, &candidate.Finder.Redis.Username, &candidate.Finder.Redis.Password)
	}
	for index := range candidate.App {
		app := &candidate.App[index]
		fields = append(fields, &app.HLS.SecureKey)
		if cfg := app.Recorder.AwsCfg; cfg != nil {
			fields = append(fields, &cfg.SecretID, &cfg.SecretKey, &cfg.SessionToken)
		}
		if cfg := app.Recorder.CosCfg; cfg != nil {
			fields = append(fields, &cfg.SecretID, &cfg.SecretKey, &cfg.SessionToken)
		}
	}
	for _, field := range fields {
		var missing bool
		value := os.Expand(*field, func(name string) string {
			value, ok := os.LookupEnv(name)
			if !ok {
				missing = true
			}
			return value
		})
		if missing {
			return errors.New("a referenced credential environment variable is not set")
		}
		*field = value
	}
	return nil
}

func NewManager(ctx context.Context, filename string) (*Manager, error) {
	absolute, err := filepath.Abs(filename)
	if err != nil {
		return nil, err
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	manager := &Manager{ctx: ctx, path: absolute, watchReady: make(chan struct{})}
	if err := manager.Reload(); err != nil {
		return nil, err
	}
	return manager, nil
}

func (manager *Manager) Version() uint64 { return manager.version.Load() }

func (manager *Manager) Reload() error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	data, err := os.ReadFile(manager.path)
	if err != nil {
		return err
	}
	candidate, err := Decode(data)
	if err != nil {
		return err
	}
	previous := Get()
	canonical, err := json.Marshal(candidate)
	if err != nil {
		return err
	}
	fingerprint := sha256.Sum256(canonical)
	if manager.Version() != 0 && fingerprint == manager.fingerprint {
		return nil
	}
	if manager.Version() != 0 && (candidate.HTTP.Addr != previous.HTTP.Addr || candidate.RTMP.Addr != previous.RTMP.Addr) {
		return errors.New("listener changes require restart; active configuration retained")
	}
	if err := candidate.InitFinder(manager.ctx); err != nil {
		candidate.Revoke()
		return fmt.Errorf("prepare discovery: %w", err)
	}
	prepareCtx, cancelPreparation := context.WithTimeout(manager.ctx, 5*time.Second)
	defer cancelPreparation()
	for index := range candidate.App {
		app := &candidate.App[index]
		if !app.Enable {
			continue
		}
		var previousRecorder *AppRecorder
		if manager.Version() != 0 {
			if previousApp := previous.GetApp(app.AppName); previousApp != nil {
				previousRecorder = &previousApp.Recorder
			}
		}
		if err := app.Recorder.prepare(prepareCtx, previousRecorder, app.AppName); err != nil {
			candidate.Revoke()
			return fmt.Errorf("prepare recording: %w", err)
		}
	}
	candidate.Finder.inheritLocalRegistrations(&previous.Finder)
	if err := manager.ctx.Err(); err != nil {
		candidate.Revoke()
		return err
	}
	candidate.Proxy.prepareSigning(&previous.Proxy, time.Now().Unix())
	if manager.Version() == 0 {
		candidate.Proxy.preparePrefixes(nil)
	} else {
		candidate.Proxy.preparePrefixes(&previous.Proxy)
	}
	candidate.Revision = manager.Version() + 1
	current.Store(candidate)
	metrics.SetCluster(candidate.Metrics.Cluster)
	base.SetReadTimeout(time.Duration(candidate.RTMP.ReadTimeout) * time.Second)
	var level slog.Level
	_ = level.UnmarshalText([]byte(candidate.LogLevel))
	logging.Level.Set(level)
	manager.version.Add(1)
	manager.fingerprint = fingerprint
	if manager.OnCommit != nil {
		manager.OnCommit(previous, candidate)
	}
	previous.Revoke()
	return nil
}

func (manager *Manager) Watch(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = watcher.Close() }()
	if err := watcher.Add(filepath.Dir(manager.path)); err != nil {
		return err
	}
	manager.watchReadyOnce.Do(func() { close(manager.watchReady) })
	var debounce <-chan time.Time
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return errors.New("configuration watcher closed")
			}
			if filepath.Clean(event.Name) == manager.path && event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
				timer.Reset(100 * time.Millisecond)
				debounce = timer.C
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return errors.New("configuration watcher errors closed")
			}
			logging.Error("configuration watcher: ", err)
		case <-debounce:
			debounce = nil
			if err := manager.Reload(); err != nil {
				logging.Error("configuration reload rejected: ", err)
			} else {
				logging.Infof("configuration version %d committed", manager.Version())
			}
		}
	}
}

func (cfg *Config) Redacted() map[string]any {
	data, _ := json.Marshal(cfg)
	var document map[string]any
	_ = json.Unmarshal(data, &document)
	var redact func(any)
	redact = func(value any) {
		switch container := value.(type) {
		case map[string]any:
			for key, field := range container {
				switch key {
				case "secret_id", "secret_key", "secure_key", "password", "session_token":
					if field != "" {
						container[key] = "[redacted]"
					}
				default:
					if text, ok := field.(string); ok && strings.Contains(text, "://") {
						if address, err := url.Parse(text); err == nil {
							address.User = nil
							address.RawQuery = ""
							address.Fragment = ""
							container[key] = address.String()
						}
					} else {
						redact(field)
					}
				}
			}
		case []any:
			for index, field := range container {
				if text, ok := field.(string); ok && strings.Contains(text, "://") {
					if address, err := url.Parse(text); err == nil {
						address.User = nil
						address.RawQuery = ""
						address.Fragment = ""
						container[index] = address.String()
					}
				} else {
					redact(field)
				}
			}
		}
	}
	redact(document)
	document["_revision"] = cfg.Revision
	document["_components"] = appstate.Snapshot()
	return document
}
