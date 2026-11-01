package storage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/appstate"
	"github.com/nextpkg/streamd/metrics"
)

type rotationDriver struct {
	started chan bool
	release chan struct{}
	once    sync.Once
	failure error
}

func (driver *rotationDriver) Open() error        { return nil }
func (driver *rotationDriver) Write([]byte) error { return nil }
func (driver *rotationDriver) Close(complete, block bool) error {
	driver.once.Do(func() { driver.started <- complete })
	if block {
		<-driver.release
		return driver.failure
	}
	return nil
}

func TestOrdinaryRotationDoesNotWaitOrReportPrematureCompletion(test *testing.T) {
	for _, mode := range []string{"complete", "pause", "failure"} {
		test.Run(mode, func(test *testing.T) {
			driver := &rotationDriver{started: make(chan bool, 1), release: make(chan struct{})}
			if mode == "failure" {
				driver.failure = errors.New("segment completion rejected")
			}
			events := make(chan string, 2)
			settings := &config.AppRecorder{SaveAs: "flv", SaveMode: "round", Segment: 3600, Driver: "aws"}
			store := NewStorage(base.NewInfo("rotation", mode, false), settings, &Event{OnRecord: func(string) {}, OnRecordDone: func(location string) { events <- location }})
			store.dr, store.fullRela = driver, "/rotation/"+mode+"/first.flv"
			store.nTime = time.Unix(settings.Normalize(time.Now().Unix()), 0)
			store.isOpen.Store(true)
			metrics.AddGlobal(metrics.Recording, 1)
			if mode != "pause" {
				store.ForceCompletion()
			}
			var releaseOnce sync.Once
			unlock := func() { releaseOnce.Do(func() { close(driver.release) }) }
			defer func() { unlock(); store.Release() }()
			returned := make(chan error, 1)
			go func() { returned <- store.Close(false) }()
			select {
			case err := <-returned:
				if err != nil {
					test.Fatal(err)
				}
			case <-time.After(200 * time.Millisecond):
				unlock()
				err := <-returned
				test.Fatalf("ordinary segment rotation waits for cloud finalization: %v", err)
			}
			select {
			case complete := <-driver.started:
				if complete != (mode != "pause") {
					test.Fatal("pause/completion decision changed")
				}
			case <-time.After(time.Second):
				test.Fatal("finalization was not started")
			}
			select {
			case <-events:
				test.Fatal("completion event preceded actual completion")
			default:
			}
			store.fullRela = "/rotation/new-generation.flv"
			joined := make(chan error, 1)
			go func() { joined <- store.Close(true) }()
			select {
			case err := <-joined:
				test.Fatalf("blocking close did not join the previous segment: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			unlock()
			select {
			case err := <-joined:
				if !errors.Is(err, driver.failure) {
					test.Fatalf("finalization result lost: %v", err)
				}
			case <-time.After(time.Second):
				test.Fatal("finalization did not retire")
			}
			if mode == "complete" {
				select {
				case location := <-events:
					if location != "/rotation/complete/first.flv" {
						test.Fatal("old completion used the new segment identity")
					}
				default:
					test.Fatal("successful completion event missing")
				}
			}
			select {
			case <-events:
				test.Fatal("pause or failure emitted success, or completion repeated")
			default:
			}
		})
	}
}

func TestRotationFailurePreservesCurrentStateAndOwner(test *testing.T) {
	for _, replaceOwner := range []bool{false, true} {
		info := base.NewInfo("rotation-status", "sample", false)
		appstate.Start("recording", info.Key, info.UID, 1, "active")
		driver := &rotationDriver{started: make(chan bool, 1), release: make(chan struct{}), failure: errors.New("old segment failed")}
		settings := &config.AppRecorder{SaveAs: "flv", SaveMode: "immediate", Segment: 4, Driver: "aws"}
		store := NewStorage(info, settings, NewEvent())
		store.dr = driver
		store.isOpen.Store(true)
		metrics.AddGlobal(metrics.Recording, 1)
		if err := store.Close(false); err != nil {
			test.Fatal(err)
		}
		<-driver.started
		owner := info.UID
		if replaceOwner {
			owner += "-replacement"
			appstate.Start("recording", info.Key, owner, 2, "active")
		} else {
			appstate.Report("recording", info.Key, owner, 2, "active")
		}
		close(driver.release)
		if err := store.Close(true); !errors.Is(err, driver.failure) {
			test.Fatalf("finalization failure not returned: %v", err)
		}
		store.Release()
		if store.Error() != nil {
			test.Fatal("old finalization poisoned the current segment")
		}
		found := false
		for _, state := range appstate.Snapshot() {
			if state.Component != "recording" || state.Stream != info.Key {
				continue
			}
			found = true
			expected := uint64(1)
			if replaceOwner {
				expected = 0
			}
			if state.Revision != 2 || state.State != "active" || state.Failures != expected || (!replaceOwner && state.LastFailureAt == 0) {
				test.Fatalf("late result changed current state or owner: %+v", state)
			}
		}
		appstate.Forget("recording", info.Key, owner)
		if !found {
			test.Fatal("current component status disappeared")
		}
	}
}
