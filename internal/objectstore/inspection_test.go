package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type inspectionQueryBackend struct {
	Backend
	query func(context.Context, string) ([]Upload, error)
}

func (backend *inspectionQueryBackend) Uploads(ctx context.Context, prefix string) ([]Upload, error) {
	return backend.query(ctx, prefix)
}

func waitInspection(test *testing.T, client *Client, check func(InspectionStats) bool) InspectionStats {
	test.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats := client.InspectionStats()
		if check(stats) {
			return stats
		}
		time.Sleep(time.Millisecond)
	}
	test.Fatalf("inspection state did not converge: %+v", client.InspectionStats())
	return InspectionStats{}
}

func TestInspectionJitterPreservesOriginalWindow(test *testing.T) {
	seen := map[time.Duration]bool{}
	for sample := 0; sample < 256; sample++ {
		delay := inspectionJitter()
		if delay < 2*time.Second || delay > 31*time.Second || delay%time.Second != 0 {
			test.Fatalf("unexpected inspection delay %v", delay)
		}
		seen[delay] = true
	}
	if len(seen) < 2 {
		test.Fatal("inspection jitter does not disperse requests")
	}
}

func TestInspectionCoalescingCapacityAndRetirement(test *testing.T) {
	client := &Client{inspectionDelay: func() time.Duration { return time.Hour }}
	defer client.Close()
	expired := func(string, int64) bool { return true }
	for repeat := 0; repeat < 100; repeat++ {
		if !client.ScheduleInspection(fmt.Sprintf("shared/%d.ts", repeat), 2, expired) {
			test.Fatal("shared inspection rejected")
		}
	}
	if stats := client.InspectionStats(); stats.Pending != 1 || stats.Coalesced != 99 {
		test.Fatalf("duplicate prefix scans: %+v", stats)
	}
	for prefix := 1; prefix < maxPendingInspections; prefix++ {
		if !client.ScheduleInspection(fmt.Sprintf("prefix-%d/1.ts", prefix), 2, expired) {
			test.Fatal("inspection rejected before capacity")
		}
	}
	if client.ScheduleInspection("overflow/1.ts", 2, expired) {
		test.Fatal("inspection capacity is unbounded")
	}
	client.Close()
	stats := client.InspectionStats()
	if stats.Pending != 0 || stats.Running != 0 || stats.Cancelled != maxPendingInspections || stats.Rejected != 1 {
		test.Fatalf("inspection retirement: %+v", stats)
	}
	if client.ScheduleInspection("retired/1.ts", 2, expired) {
		test.Fatal("retired client accepted inspection")
	}
}

func TestInspectionBoundsConcurrencyAndCancelsIO(test *testing.T) {
	var active, peak atomic.Int64
	backend := &inspectionQueryBackend{query: func(ctx context.Context, _ string) ([]Upload, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	client := &Client{Backend: backend, inspectionDelay: func() time.Duration { return 0 }}
	defer client.Close()
	for prefix := 0; prefix < 8; prefix++ {
		client.ScheduleInspection(fmt.Sprintf("prefix-%d/1.ts", prefix), 2, func(string, int64) bool { return true })
	}
	waitInspection(test, client, func(stats InspectionStats) bool {
		return stats.Running == inspectionParallelism && active.Load() == inspectionParallelism
	})
	client.Close()
	stats := client.InspectionStats()
	if peak.Load() > inspectionParallelism || active.Load() != 0 || stats.Pending != 0 || stats.Running != 0 || stats.Cancelled != 8 {
		test.Fatalf("cancellation/concurrency mismatch: peak=%d active=%d stats=%+v", peak.Load(), active.Load(), stats)
	}
}

func TestInspectionFailureRemainsObservableAndRetryable(test *testing.T) {
	var failed atomic.Bool
	failed.Store(true)
	backend := &inspectionQueryBackend{query: func(context.Context, string) ([]Upload, error) {
		if failed.Load() {
			return nil, errors.New("injected inspection failure")
		}
		return nil, nil
	}}
	client := &Client{Backend: backend, inspectionDelay: func() time.Duration { return 0 }}
	defer client.Close()
	client.ScheduleInspection("retry/1.ts", 2, func(string, int64) bool { return true })
	stats := waitInspection(test, client, func(stats InspectionStats) bool { return stats.Pending == 0 && stats.Failed == 1 })
	if !strings.Contains(stats.LastError, "injected inspection failure") {
		test.Fatal("inspection failure disappeared")
	}
	failed.Store(false)
	client.ScheduleInspection("retry/2.ts", 2, func(string, int64) bool { return true })
	waitInspection(test, client, func(stats InspectionStats) bool {
		return stats.Pending == 0 && stats.Completed == 1 && stats.Failed == 1
	})
}

func TestRecordingClaimsSurviveClientRetirementAndKeyRotation(test *testing.T) {
	first, err := NewS3(&S3Config{Enable: true, Endpoint: "http://127.0.0.1:1", Bucket: "bucket", SecretID: "first", SecretKey: "first"})
	if err != nil {
		test.Fatal(err)
	}
	defer first.Close()
	second, err := NewS3(&S3Config{Enable: true, Endpoint: "http://127.0.0.1:1/", Bucket: "bucket", SecretID: "rotated", SecretKey: "rotated"})
	if err != nil {
		test.Fatal(err)
	}
	defer second.Close()
	release, err := first.ClaimRecording("folder/../recording.ts")
	if err != nil {
		test.Fatal(err)
	}
	defer release()
	first.Close()
	if _, err := second.ClaimRecording("/recording.ts"); !errors.Is(err, ErrRecordingBusy) {
		test.Fatalf("retired client's live recording became unprotected: %v", err)
	}
	release()
	release()
	next, err := second.ClaimRecording("recording.ts")
	if err != nil {
		test.Fatal(err)
	}
	next()
}

type inspectionManifestBackend struct {
	*checkpointStub
	lists, partLists, completes, aborts int
	manifest                            []Part
	tail                                []byte
	badKey                              string
}

func (backend *inspectionManifestBackend) Uploads(context.Context, string) ([]Upload, error) {
	backend.lists++
	return slices.Clone(backend.uploads), nil
}
func (backend *inspectionManifestBackend) Parts(context.Context, string, string) ([]Part, error) {
	backend.partLists++
	return slices.Clone(backend.parts), nil
}
func (backend *inspectionManifestBackend) ReadCheckpoint(ctx context.Context, key string) ([]byte, error) {
	if backend.badKey != "" && key == checkpointKey(backend.badKey) {
		return []byte("broken checkpoint"), nil
	}
	return backend.checkpointStub.ReadCheckpoint(ctx, key)
}
func (backend *inspectionManifestBackend) Upload(_ context.Context, _ string, _ string, number int32, data []byte) (Part, error) {
	backend.tail = bytes.Clone(data)
	return Part{Number: number, ETag: "tail-part", Size: int64(len(data))}, nil
}
func (backend *inspectionManifestBackend) Complete(_ context.Context, _ string, _ string, parts []Part) error {
	backend.completes++
	backend.manifest = slices.Clone(parts)
	return nil
}
func (backend *inspectionManifestBackend) Abort(context.Context, string, string) error {
	backend.aborts++
	return nil
}

func manifestInspectionClient(test *testing.T, saved checkpoint) (*Client, *inspectionManifestBackend) {
	test.Helper()
	body, err := json.Marshal(saved)
	if err != nil {
		test.Fatal(err)
	}
	backend := &inspectionManifestBackend{checkpointStub: &checkpointStub{saved: body, uploads: []Upload{{Key: saved.Key, ID: saved.ID}}, parts: slices.Clone(saved.Parts)}}
	client := &Client{Backend: backend, leaseScope: test.Name()}
	test.Cleanup(client.Close)
	return client, backend
}

func TestExpiredInspectionUsesOneListingAndPreservesTail(test *testing.T) {
	saved := validCheckpoint()
	client, backend := manifestInspectionClient(test, saved)
	result, err := client.InspectExpired(context.Background(), saved.Key, 900, func(string, int64) bool { return true })
	if err != nil || result.Finalized != 1 || backend.lists != 1 || backend.partLists != 1 || backend.completes != 1 || backend.aborts != 0 || !bytes.Equal(backend.tail, saved.Tail) || len(backend.manifest) != 2 || backend.saved != nil {
		test.Fatalf("inspection lost data or amplified listing: result=%+v err=%v backend=%+v", result, err, backend)
	}
}

func TestExpiredInspectionSkipsActiveAndAmbiguousObjects(test *testing.T) {
	saved := validCheckpoint()
	client, backend := manifestInspectionClient(test, saved)
	other := &Client{Backend: backend, leaseScope: test.Name()}
	defer other.Close()
	release, err := other.ClaimRecording(saved.Key)
	if err != nil {
		test.Fatal(err)
	}
	result, err := client.InspectExpired(context.Background(), saved.Key, 900, func(string, int64) bool { return true })
	release()
	if err != nil || result.Skipped != 1 || backend.partLists != 0 || backend.completes != 0 {
		test.Fatalf("active object changed: %+v %v", result, err)
	}
	backend.uploads = append(backend.uploads, Upload{Key: saved.Key, ID: "another-upload"})
	_, err = client.InspectExpired(context.Background(), saved.Key, 900, func(string, int64) bool { return true })
	if err == nil || backend.partLists != 0 || backend.completes != 0 || backend.aborts != 0 {
		test.Fatal("ambiguous remote object was changed")
	}
}

func TestExpiredInspectionIsolatesCorruptionAndNestedPrefixes(test *testing.T) {
	saved := validCheckpoint()
	client, backend := manifestInspectionClient(test, saved)
	backend.badKey = "app/stream/1.flv"
	backend.uploads = append([]Upload{{Key: backend.badKey, ID: "bad"}, {Key: "app/stream/nested/1.flv", ID: "nested"}}, backend.uploads...)
	result, err := client.InspectExpired(context.Background(), saved.Key, 900, func(string, int64) bool { return true })
	if err == nil || result.Finalized != 1 || backend.partLists != 1 || backend.aborts != 0 || backend.completes != 1 {
		test.Fatalf("bad neighbor blocked or escaped inspection: %+v %v", result, err)
	}
}

func TestExpiredInspectionRemovesCheckpointedEmptyUpload(test *testing.T) {
	saved := validCheckpoint()
	saved.Parts = nil
	saved.Tail = nil
	saved.Digest = fmt.Sprintf("%x", sha256.Sum256(nil))
	client, backend := manifestInspectionClient(test, saved)
	result, err := client.InspectExpired(context.Background(), saved.Key, 900, func(string, int64) bool { return true })
	if err != nil || result.Finalized != 1 || backend.aborts != 1 || backend.completes != 0 || backend.saved != nil {
		test.Fatalf("empty upload not retired: %+v %v", result, err)
	}
}
