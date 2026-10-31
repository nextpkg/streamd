package rtmp

import (
	"context"
	"fmt"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/rtmp/core"
)

type originFlight struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (connection *Conn) installOriginWriter(writer base.WriteCloser) {
	key := writer.Info().Key
	lock := connection.keyLock(key)
	lock.Lock()
	if connection.closed.Load() {
		lock.Unlock()
		writer.Close()
		return
	}
	if value, ok := connection.stream.Load(key); ok {
		stream := value.(*Stream)
		if stream.Reader() != nil {
			err := stream.addWriter(defaultStreamWriter(writer))
			connection.checkin.Store(time.Now().Unix())
			lock.Unlock()
			if err != nil {
				writer.Close()
			}
			return
		}
	}
	cfg := connection.currentConfig()
	if !writer.Info().IsExternal || !cfg.Finder.EnableRelay || cfg.GetApp(writer.Info().App) == nil {
		lock.Unlock()
		writer.Close()
		return
	}
	value, waiting := connection.origins.Load(key)
	var flight *originFlight
	if waiting {
		flight = value.(*originFlight)
	} else {
		ctx, cancel := context.WithCancel(connection.ctx)
		flight = &originFlight{ctx: ctx, cancel: cancel, done: make(chan struct{})}
		connection.origins.Store(key, flight)
	}
	lock.Unlock()
	if !waiting {
		func() {
			defer func() {
				flight.cancel()
				lock.Lock()
				connection.origins.CompareAndDelete(key, flight)
				close(flight.done)
				lock.Unlock()
			}()
			connection.captureOrigin(writer.Info().Copy(), cfg, flight)
		}()
	} else {
		select {
		case <-flight.done:
		case <-connection.ctx.Done():
			writer.Close()
			return
		}
	}
	lock.Lock()
	defer lock.Unlock()
	if !connection.closed.Load() {
		if value, ok := connection.stream.Load(key); ok {
			stream := value.(*Stream)
			if stream.Reader() != nil && stream.addWriter(defaultStreamWriter(writer)) == nil {
				connection.checkin.Store(time.Now().Unix())
				return
			}
		}
	}
	writer.Close()
}

func (connection *Conn) captureOrigin(info *base.Info, cfg *config.Config, flight *originFlight) {
	info.IsExternal = true
	candidates := connection.originSources(info, cfg)
	if candidates == nil {
		return
	}
	for _, candidate := range candidates.data {
		if flight.ctx.Err() != nil {
			return
		}
		if !candidate.usable && time.Now().Unix()-candidate.deadTime < cacheTTL {
			continue
		}
		timeout := time.Duration(cfg.RTMP.DialTimeout) * time.Second
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		ctx, cancel := context.WithTimeout(flight.ctx, timeout)
		// Preserve the flight's source/timeout snapshot, but refresh transport options
		// for each new dial, including retries after a configuration reload.
		settings := connection.currentConfig().RTMP
		options := core.ClientOptions{BufferSize: settings.BufSize, SlabMin: settings.SlabMin * 1024, SlabMax: settings.SlabMax * 1024}
		reader, err := connection.pull(ctx, info.Copy(), candidate.pullHost, timeout, options)
		cancel()
		if err != nil {
			candidate.usable, candidate.deadTime = false, time.Now().Unix()
			log.Debugf("[%s]origin failed: %v", info.Key, err)
			continue
		}
		candidate.usable = true
		connection.attachOrigin(info, reader, flight, candidate.pullHost)
		return
	}
}

func (connection *Conn) originSources(info *base.Info, cfg *config.Config) *sourceCache {
	if previous, ok := connection.source.Load(info.Key); ok {
		cached := previous.(*sourceCache)
		if cached.config == cfg && cached.app == cfg.GetApp(info.App) && time.Now().Unix()-cached.time <= int64(cfg.Finder.Expire) {
			return cached
		}
	}
	candidates := &sourceCache{time: time.Now().Unix(), config: cfg, app: cfg.GetApp(info.App)}
	if !connection.findWithConfig(candidates, info, cfg) {
		return nil
	}
	connection.source.Store(info.Key, candidates)
	return candidates
}

func (connection *Conn) attachOrigin(info *base.Info, reader base.ReadCloser, flight *originFlight, address string) {
	lock := connection.keyLock(info.Key)
	lock.Lock()
	current, exists := connection.origins.Load(info.Key)
	if connection.closed.Load() || flight.ctx.Err() != nil || !exists || current != flight {
		lock.Unlock()
		reader.Close()
		return
	}
	if current, exists := connection.stream.Load(info.Key); exists {
		previous := current.(*Stream)
		if previous.Reader() != nil {
			lock.Unlock()
			reader.Close()
			return
		}
		_ = previous.terminate(true)
	}
	// Re-check admission after the dial; the application may have been removed.
	cfg := connection.currentConfig()
	app := cfg.GetApp(info.App)
	if app == nil {
		lock.Unlock()
		reader.Close()
		return
	}
	stream := newStream(info, cfg, app)
	if err := stream.addReader(&StreamReader{ReadCloser: reader}); err != nil {
		lock.Unlock()
		reader.Close()
		return
	}
	connection.stream.Store(info.Key, stream)
	lock.Unlock()
	log.Infof("[%s]Origin attached from %s", info.Key, address)
	for _, player := range connection.players {
		if player == nil {
			continue
		}
		internal := info.Copy()
		internal.IsExternal = false
		writer, err := player.GetWriter(internal, reader.GetConnect())
		if err != nil {
			log.Errorf("[%s]origin embedded player: %v", info.Key, err)
			continue
		}
		if writer != nil {
			connection.InstallWriter(writer)
		}
	}
}

func pullOrigin(ctx context.Context, info *base.Info, address string, timeout time.Duration, options core.ClientOptions) (base.ReadCloser, error) {
	client, err := core.DialConnClient(ctx, address, timeout, options)
	if err != nil {
		return nil, err
	}
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = client.Close(); close(closed) })
	reader := core.NewReader(client)
	*reader.Info() = *info
	err = client.StartPlay()
	if err == nil {
		err = reader.Prefetch()
	}
	if !stop() {
		<-closed
		return nil, fmt.Errorf("origin startup cancelled: %w", ctx.Err())
	}
	if err != nil || ctx.Err() != nil {
		_ = client.Close()
		if err == nil {
			err = ctx.Err()
		}
		return nil, err
	}
	return reader, nil
}
