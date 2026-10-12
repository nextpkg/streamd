package driver

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/internal/mediatime"
	"github.com/nextpkg/streamd/internal/objectstore"
)

const blockSize = objectstore.MinBlockSize
const ioChanSize = 4

type Driver interface {
	Open() error
	Write([]byte) error
	Close(bool, bool) error
}

type Custom interface {
	Open() error
	Upload([]byte) error
	Complete() error
}

type resumable interface {
	ResumeTail() []byte
	Suspend([]byte) error
}

type mediaResumable interface {
	ResumeMedia() (*mediatime.State, int64)
	SuspendMedia([]byte, *mediatime.State) error
}

type Config struct {
	buf          []byte
	ch           chan []byte
	done         chan struct{}
	custom       Custom
	mu           sync.Mutex
	errMu        sync.Mutex
	err          error
	opened       bool
	closed       bool
	complete     bool
	pending      []byte
	media        *mediatime.State
	existing     *mediatime.State
	size         int64
	existingSize int64
}

func NewConfig(custom Custom) Driver {
	return &Config{ch: make(chan []byte, ioChanSize), done: make(chan struct{}), custom: custom}
}

func (cfg *Config) Open() error {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	if cfg.closed {
		return errors.New("storage driver is closed")
	}
	if cfg.opened {
		return nil
	}
	if err := cfg.custom.Open(); err != nil {
		return err
	}
	if saved, ok := cfg.custom.(mediaResumable); ok {
		cfg.existing, cfg.existingSize = saved.ResumeMedia()
		cfg.media, cfg.size = cfg.existing.Clone(), cfg.existingSize
	}
	if saved, ok := cfg.custom.(resumable); ok {
		cfg.buf = saved.ResumeTail()
		if len(cfg.buf) >= blockSize {
			return errors.New("restored recording tail exceeds block size")
		}
	}
	cfg.opened = true
	go cfg.process()
	return nil
}

func (cfg *Config) failure() error {
	cfg.errMu.Lock()
	defer cfg.errMu.Unlock()
	return cfg.err
}

func (cfg *Config) fail(err error) {
	if err == nil {
		return
	}
	cfg.errMu.Lock()
	defer cfg.errMu.Unlock()
	if cfg.err == nil {
		cfg.err = err
	}
}

func (cfg *Config) process() {
	defer close(cfg.done)
	for data := range cfg.ch {
		if cfg.failure() == nil {
			cfg.fail(cfg.custom.Upload(data))
		}
	}
	if cfg.failure() == nil && cfg.complete {
		cfg.fail(cfg.custom.Complete())
	} else if cfg.failure() == nil {
		if saved, ok := cfg.custom.(mediaResumable); ok {
			cfg.fail(saved.SuspendMedia(cfg.pending, cfg.media))
		} else if saved, ok := cfg.custom.(resumable); ok {
			cfg.fail(saved.Suspend(cfg.pending))
		} else if len(cfg.pending) != 0 {
			cfg.fail(errors.New("recording driver cannot preserve an unfinished tail"))
		}
	}
	if cfg.failure() != nil {
		if aborter, ok := cfg.custom.(interface{ Abort() error }); ok {
			cfg.fail(aborter.Abort())
		}
	}
	cfg.pending = nil
}

func (cfg *Config) enqueue(data []byte) error {
	select {
	case cfg.ch <- data:
		return nil
	default:
		err := errors.New("recording upload queue capacity exceeded")
		cfg.fail(err)
		return err
	}
}

func (cfg *Config) Write(data []byte) error {
	return cfg.writeMedia(data, nil)
}

func (cfg *Config) WriteMedia(data []byte, state mediatime.State) error {
	return cfg.writeMedia(data, &state)
}

func (cfg *Config) ResumeMedia() (*mediatime.State, int64) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	return cfg.existing.Clone(), cfg.existingSize
}

func (cfg *Config) writeMedia(data []byte, state *mediatime.State) error {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	if !cfg.opened || cfg.closed {
		return errors.New("storage driver is not open")
	}
	if err := cfg.failure(); err != nil {
		return err
	}
	length := len(data)
	for len(data) != 0 {
		count := min(blockSize-len(cfg.buf), len(data))
		cfg.buf = append(cfg.buf, data[:count]...)
		data = data[count:]
		if len(cfg.buf) == blockSize {
			buffer := cfg.buf
			cfg.buf = nil
			if err := cfg.enqueue(buffer); err != nil {
				return err
			}
		}
	}
	cfg.size += int64(length)
	if length != 0 {
		cfg.media = state.Clone()
		if cfg.media != nil {
			cfg.media.Bytes = cfg.size
		}
	}
	return nil
}

func (cfg *Config) Close(complete, block bool) error {
	cfg.mu.Lock()
	if !cfg.closed {
		cfg.closed = true
		if cfg.opened {
			if len(cfg.buf) != 0 && complete {
				_ = cfg.enqueue(cfg.buf)
			} else if !complete {
				cfg.pending = cfg.buf
			}
			cfg.buf = nil
			cfg.complete = complete
			close(cfg.ch)
		} else {
			close(cfg.done)
		}
	}
	cfg.mu.Unlock()
	if block {
		<-cfg.done
	}
	return cfg.failure()
}

func IsExpired(key string, segment int64) bool { return isExpiredAt(key, segment, time.Now()) }

func isExpiredAt(key string, segment int64, now time.Time) bool {
	filename := filepath.Base(key)
	separator := strings.LastIndexByte(filename, '.')
	if separator <= 0 || segment <= 0 {
		return false
	}
	timestamp, err := strconv.ParseInt(filename[:separator], 10, 64)
	return err == nil && now.Unix()-timestamp > 2*segment
}
