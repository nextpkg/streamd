package objectstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientOwnersRetireMaintenanceOnlyAfterLastRelease(test *testing.T) {
	client := &Client{inspectionDelay: func() time.Duration { return time.Hour }}
	defer client.Close()
	first, second := client.Retain(), client.Retain()
	defer first()
	defer second()
	require.True(test, client.ScheduleInspection("live/1.ts", 4, func(string, int64) bool { return true }))
	client.Close()
	require.Equal(test, 1, client.InspectionStats().Pending)
	first()
	first()
	require.Equal(test, 1, client.InspectionStats().Pending)
	second()
	require.Zero(test, client.InspectionStats().Pending)
	require.Equal(test, 1, client.InspectionStats().Cancelled)
	require.False(test, client.ScheduleInspection("live/2.ts", 4, func(string, int64) bool { return true }))
}

func TestClientOwnerFromRetiredSnapshotHasBoundedLifetime(test *testing.T) {
	client := &Client{inspectionDelay: func() time.Duration { return time.Hour }}
	client.Close()
	for generation := 0; generation < 3; generation++ {
		release := client.Retain()
		require.True(test, client.ScheduleInspection("live/1.ts", 4, func(string, int64) bool { return true }))
		release()
		require.Zero(test, client.InspectionStats().Pending)
		require.Equal(test, 1, client.InspectionStats().Cancelled)
		require.False(test, client.ScheduleInspection("live/2.ts", 4, func(string, int64) bool { return true }))
	}
}

func TestLastClientOwnerJoinsCancelledMaintenance(test *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	client := &Client{inspectionDelay: func() time.Duration { return 0 }, Backend: &inspectionQueryBackend{query: func(ctx context.Context, _ string) ([]Upload, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}}}
	release := client.Retain()
	defer release()
	defer client.Close()
	require.True(test, client.ScheduleInspection("live/1.ts", 4, func(string, int64) bool { return true }))
	select {
	case <-started:
	case <-time.After(time.Second):
		test.Fatal("maintenance did not start")
	}
	client.Close()
	select {
	case <-cancelled:
		test.Fatal("configuration retirement cancelled an owned recording")
	default:
	}
	release()
	select {
	case <-cancelled:
	default:
		test.Fatal("last owner did not join cancelled maintenance")
	}
	require.Zero(test, client.InspectionStats().Pending)
	require.Zero(test, client.InspectionStats().Running)
}

func TestClientReacquisitionJoinsPreviousMaintenanceGeneration(test *testing.T) {
	started, cancelled, unblock := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finishOld := sync.OnceFunc(func() { close(unblock) })
	var requests atomic.Int32
	client := &Client{inspectionDelay: func() time.Duration { return 0 }, Backend: &inspectionQueryBackend{query: func(ctx context.Context, _ string) ([]Upload, error) {
		if requests.Add(1) != 1 {
			return nil, nil
		}
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-unblock
		return nil, ctx.Err()
	}}}
	defer client.Close()
	defer finishOld()
	require.True(test, client.ScheduleInspection("old/1.ts", 4, func(string, int64) bool { return true }))
	select {
	case <-started:
	case <-time.After(time.Second):
		test.Fatal("old maintenance did not start")
	}
	closed := make(chan struct{})
	go func() { client.Close(); close(closed) }()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		test.Fatal("old maintenance was not cancelled")
	}
	acquired, released, releaseNew := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finishNew := sync.OnceFunc(func() { close(releaseNew) })
	go func() {
		release := client.Retain()
		close(acquired)
		<-releaseNew
		release()
		close(released)
	}()
	test.Cleanup(func() {
		finishOld()
		finishNew()
		<-closed
		<-released
	})
	select {
	case <-acquired:
		test.Fatal("client reopened while previous maintenance was still draining")
	case <-time.After(50 * time.Millisecond):
	}
	finishOld()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		test.Fatal("client did not reopen after previous maintenance joined")
	}
	<-closed
	require.True(test, client.ScheduleInspection("new/1.ts", 4, func(string, int64) bool { return true }))
	waitInspection(test, client, func(stats InspectionStats) bool { return stats.Completed == 1 && stats.Pending == 0 })
	finishNew()
	<-released
	require.Equal(test, int32(2), requests.Load())
	require.Zero(test, client.InspectionStats().Pending)
	require.Zero(test, client.InspectionStats().Running)
}

func TestRecordingRefreshFallbackAndFailedOpenPreserveOwnership(test *testing.T) {
	backend := &checkpointStub{}
	client := &Client{Backend: backend, renew: func() (*Client, error) { return nil, errors.New("injected constructor failure") }}
	defer client.Close()
	upload, release, err := client.NewRecordingUpload(context.Background(), "recording.ts", true)
	require.NoError(test, err)
	require.Same(test, client, upload.client)
	release()
	release()
	require.False(test, client.retired)
	fresh := &Client{Backend: &checkpointStub{readErr: errors.New("injected checkpoint failure")}}
	client.renew = func() (*Client, error) { return fresh, nil }
	_, release, err = client.NewRecordingUpload(context.Background(), "recording.ts", true)
	require.ErrorContains(test, err, "injected checkpoint failure")
	require.Nil(test, release)
	require.True(test, fresh.retired)
	claim, err := client.ClaimRecording("recording.ts")
	require.NoError(test, err)
	claim()
}

func TestInspectionRefreshClosesOnlyItsPrivateClient(test *testing.T) {
	var renewed atomic.Pointer[Client]
	var requests atomic.Int32
	client := &Client{inspectionDelay: func() time.Duration { return 0 }, renew: func() (*Client, error) {
		fresh := &Client{Backend: &inspectionQueryBackend{query: func(context.Context, string) ([]Upload, error) {
			requests.Add(1)
			return nil, nil
		}}}
		renewed.Store(fresh)
		return fresh, nil
	}}
	defer client.Close()
	require.True(test, client.ScheduleInspection("live/1.ts", 4, func(string, int64) bool { return true }))
	waitInspection(test, client, func(stats InspectionStats) bool { return stats.Completed == 1 && stats.Pending == 0 })
	require.Equal(test, int32(1), requests.Load())
	require.True(test, renewed.Load().retired)
	require.False(test, client.retired)
}

func TestClientRefreshUsesImmutableConfigurationSnapshot(test *testing.T) {
	awsConfig := &S3Config{Enable: true, Endpoint: "http://127.0.0.1:1", Bucket: "original", SecretID: "original", SecretKey: "original"}
	awsClient, err := NewS3(awsConfig)
	require.NoError(test, err)
	defer awsClient.Close()
	*awsConfig = S3Config{}
	awsNext := awsClient.freshClient()
	defer awsNext.Close()
	require.NotSame(test, awsClient, awsNext)
	require.Equal(test, awsClient.leaseScope, awsNext.leaseScope)
	cosConfig := &COSConfig{Enable: true, Endpoint: "http://127.0.0.1:1", SecretID: "original", SecretKey: "original"}
	cosClient, err := NewCOS(cosConfig)
	require.NoError(test, err)
	defer cosClient.Close()
	*cosConfig = COSConfig{}
	cosNext := cosClient.freshClient()
	defer cosNext.Close()
	require.NotSame(test, cosClient, cosNext)
	require.Equal(test, cosClient.leaseScope, cosNext.leaseScope)
}

func TestConcurrentClientOwnersAndRetirementJoinAllJobs(test *testing.T) {
	client := &Client{inspectionDelay: func() time.Duration { return time.Hour }}
	var workers sync.WaitGroup
	for worker := 0; worker < 64; worker++ {
		workers.Go(func() {
			release := client.Retain()
			client.ScheduleInspection("live/1.ts", 4, func(string, int64) bool { return true })
			client.Close()
			release()
		})
	}
	workers.Wait()
	client.Close()
	require.Zero(test, client.owners)
	require.Zero(test, client.InspectionStats().Pending)
	require.Zero(test, client.InspectionStats().Running)
	require.True(test, client.retired)
	require.True(test, client.inspectionClosed)
}
