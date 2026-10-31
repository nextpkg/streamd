package storage

import (
	"sync"

	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/appstate"
	errors "github.com/nextpkg/streamd/internal/errutil"
	log "github.com/nextpkg/streamd/internal/logging"
)

var ErrFinalizationPending = errors.New("recording is waiting for prior segment finalization")

type FinalizationBudget struct {
	mu     sync.Mutex
	active map[string]bool
}

func NewFinalizationBudget() *FinalizationBudget {
	return &FinalizationBudget{active: make(map[string]bool)}
}

func (budget *FinalizationBudget) reserve(key string) (func(), error) {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if len(budget.active) >= 4 || budget.active[key] {
		return nil, ErrFinalizationPending
	}
	budget.active[key] = true
	var once sync.Once
	return func() {
		once.Do(func() {
			budget.mu.Lock()
			delete(budget.active, key)
			budget.mu.Unlock()
		})
	}, nil
}

type segmentFinalization struct {
	done    chan struct{}
	failure error
}

func (store *Storage) SetFinalizationBudget(budget *FinalizationBudget) {
	store.finalizationBudget = budget
}

func (store *Storage) WaitingFinalization() bool {
	return store != nil && errors.Is(store.lastError, ErrFinalizationPending)
}

func (store *Storage) reserveCloudRecording() error {
	key := ""
	switch store.cfg.Driver {
	case config.DriverAws:
		if settings := store.cfg.AwsCfg; settings != nil {
			key = "aws\x00" + settings.Endpoint + "\x00" + settings.Bucket + "\x00" + settings.Path + store.fullRela
		}
	case config.DriverCos:
		if settings := store.cfg.CosCfg; settings != nil {
			key = "cos\x00" + settings.Endpoint + "\x00" + settings.Path + store.fullRela
		}
	default:
		return nil
	}
	if key == "" {
		return errors.New("cloud recording settings are missing")
	}
	releaseSlot, err := store.finalizationBudget.reserve(key)
	if err != nil {
		return err
	}
	releaseClients := store.cfg.RetainClients()
	store.releaseDriver = func() { releaseClients(); releaseSlot() }
	return nil
}

func (store *Storage) reapFinalizations(wait bool) error {
	pending := store.finalizations[:0]
	for _, segment := range store.finalizations {
		if wait {
			<-segment.done
		}
		select {
		case <-segment.done:
			if store.finalizationError == nil {
				store.finalizationError = segment.failure
			}
		default:
			pending = append(pending, segment)
		}
	}
	clear(store.finalizations[len(pending):])
	store.finalizations = pending
	return store.finalizationError
}

func (store *Storage) finalizeSegment(complete, block bool, prior error) error {
	driver, location, completed := store.dr, store.fullRela, store.event.OnRecordDone
	release := store.releaseDriver
	store.releaseDriver = nil
	finish := func() error {
		err := errors.Join(prior, driver.Close(complete, true))
		if err != nil {
			return errors.Wrapf(err, "close failed,sync=%t,block=%t", complete, block)
		}
		if complete {
			completed(location)
		}
		return nil
	}
	if complete {
		store.append = false
	}
	if block || (store.cfg.Driver != config.DriverAws && store.cfg.Driver != config.DriverCos) {
		if release != nil {
			defer release()
		}
		return finish()
	}
	segment := &segmentFinalization{done: make(chan struct{})}
	store.finalizations = append(store.finalizations, segment)
	key, owner := store.info.Key, store.info.UID
	go func() {
		defer close(segment.done)
		if release != nil {
			defer release()
		}
		segment.failure = finish()
		if segment.failure != nil {
			log.Errorf("[%s]recording segment finalization: %s", key, segment.failure)
			appstate.ReportBackgroundFailure("recording", key, owner)
		}
	}()
	return nil
}
