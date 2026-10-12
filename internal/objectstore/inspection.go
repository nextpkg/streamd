package objectstore

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path"
	"sync"
	"time"

	log "github.com/nextpkg/streamd/internal/logging"
)

const maxPendingInspections = 256
const inspectionParallelism = 2

var ErrRecordingBusy = errors.New("recording object is active in this process")

var recordingLeases = struct {
	sync.Mutex
	keys map[string]bool
}{keys: make(map[string]bool)}

func (client *Client) ClaimRecording(key string) (func(), error) {
	scope := client.leaseScope
	if scope == "" {
		scope = fmt.Sprintf("local:%p", client)
	}
	identity := scope + "\x00" + client.CleanKey(key)
	recordingLeases.Lock()
	defer recordingLeases.Unlock()
	if recordingLeases.keys[identity] {
		return nil, ErrRecordingBusy
	}
	recordingLeases.keys[identity] = true
	var released sync.Once
	return func() {
		released.Do(func() { recordingLeases.Lock(); delete(recordingLeases.keys, identity); recordingLeases.Unlock() })
	}, nil
}

type InspectionResult struct{ Finalized, Skipped int }

type InspectionStats struct {
	Pending, Running, Completed, Failed, Cancelled, Coalesced, Rejected, Finalized, Skipped int
	LastError                                                                               string
}

type inspectionKey struct {
	prefix  string
	segment int64
}

type inspectionState struct {
	ctx     context.Context
	cancel  context.CancelFunc
	workers sync.WaitGroup
	jobs    map[inspectionKey]bool
	slots   chan struct{}
	stats   InspectionStats
}

func inspectionPrefix(key string) string {
	directory := path.Dir(key)
	if directory == "." {
		return ""
	}
	return directory + "/"
}

func inspectionJitter() time.Duration { return time.Duration(2+rand.IntN(30)) * time.Second }

func (client *Client) InspectionStats() InspectionStats {
	client.inspectionMu.Lock()
	defer client.inspectionMu.Unlock()
	if client.inspection == nil {
		return InspectionStats{}
	}
	stats := client.inspection.stats
	stats.Pending = len(client.inspection.jobs)
	return stats
}

func (client *Client) ScheduleInspection(key string, segment int64, expired func(string, int64) bool) bool {
	client.inspectionMu.Lock()
	defer client.inspectionMu.Unlock()
	if client.inspectionClosed || segment <= 0 || expired == nil {
		return false
	}
	if client.inspection == nil {
		ctx, cancel := context.WithCancel(context.Background())
		client.inspection = &inspectionState{ctx: ctx, cancel: cancel, jobs: make(map[inspectionKey]bool), slots: make(chan struct{}, inspectionParallelism)}
	}
	state := client.inspection
	job := inspectionKey{prefix: inspectionPrefix(client.CleanKey(key)), segment: segment}
	if state.jobs[job] {
		state.stats.Coalesced++
		return true
	}
	if len(state.jobs) >= maxPendingInspections {
		state.stats.Rejected++
		log.Warnf("recording inspection queue full; future recordings will retry")
		return false
	}
	delay := inspectionJitter()
	if client.inspectionDelay != nil {
		delay = client.inspectionDelay()
	}
	state.jobs[job] = true
	state.workers.Add(1)
	go client.runInspection(state, job, expired, delay)
	return true
}

func (client *Client) runInspection(state *inspectionState, job inspectionKey, expired func(string, int64) bool, delay time.Duration) {
	defer state.workers.Done()
	defer func() { client.inspectionMu.Lock(); delete(state.jobs, job); client.inspectionMu.Unlock() }()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	started := false
	var result InspectionResult
	var err error
	defer func() {
		client.inspectionMu.Lock()
		if started {
			state.stats.Running--
		}
		state.stats.Finalized += result.Finalized
		state.stats.Skipped += result.Skipped
		if err == nil {
			state.stats.Completed++
		} else if errors.Is(err, context.Canceled) {
			state.stats.Cancelled++
		} else {
			state.stats.Failed++
			state.stats.LastError = err.Error()
		}
		client.inspectionMu.Unlock()
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warnf("recording inspection failed prefix=%s: %v", job.prefix, err)
		} else if err == nil {
			log.Infof("recording inspection completed prefix=%s finalized=%d skipped=%d", job.prefix, result.Finalized, result.Skipped)
		}
	}()
	select {
	case <-state.ctx.Done():
		err = state.ctx.Err()
		return
	case <-timer.C:
	}
	select {
	case <-state.ctx.Done():
		err = state.ctx.Err()
		return
	case state.slots <- struct{}{}:
	}
	defer func() { <-state.slots }()
	client.inspectionMu.Lock()
	state.stats.Running++
	client.inspectionMu.Unlock()
	started = true
	ctx, cancel := context.WithTimeout(state.ctx, 30*time.Second)
	defer cancel()
	fresh := client.freshClient()
	if fresh != client {
		defer fresh.Close()
	}
	result, err = fresh.inspectExpired(ctx, job.prefix, job.segment, expired)
}

func (client *Client) InspectExpired(ctx context.Context, key string, segment int64, expired func(string, int64) bool) (InspectionResult, error) {
	if segment <= 0 || expired == nil {
		return InspectionResult{}, errors.New("invalid recording inspection policy")
	}
	return client.inspectExpired(ctx, inspectionPrefix(client.CleanKey(key)), segment, expired)
}

func (client *Client) inspectExpired(ctx context.Context, prefix string, segment int64, expired func(string, int64) bool) (InspectionResult, error) {
	var result InspectionResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	uploads, err := client.Backend.Uploads(ctx, prefix)
	if err != nil {
		return result, err
	}
	counts := make(map[string]int)
	for _, upload := range uploads {
		counts[upload.Key]++
	}
	var failures error
	for _, listed := range uploads {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(failures, err)
		}
		if inspectionPrefix(listed.Key) != prefix || !expired(listed.Key, segment) {
			continue
		}
		if counts[listed.Key] != 1 {
			failures = errors.Join(failures, fmt.Errorf("ambiguous expired recording %s", listed.Key))
			continue
		}
		release, err := client.ClaimRecording(listed.Key)
		if errors.Is(err, ErrRecordingBusy) {
			result.Skipped++
			continue
		}
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		err = func() error {
			defer release()
			saved, err := client.loadCheckpoint(ctx, listed.Key)
			if err != nil {
				return err
			}
			upload, err := client.restoreMultipartUpload(ctx, listed.Key, listed.ID, saved, true)
			if err != nil {
				return err
			}
			if err := upload.Upload(ctx, upload.TakeTail()); err != nil {
				return err
			}
			return upload.Complete(ctx)
		}()
		if err != nil {
			failures = errors.Join(failures, fmt.Errorf("inspect recording %s: %w", listed.Key, err))
		} else {
			result.Finalized++
		}
	}
	return result, failures
}
