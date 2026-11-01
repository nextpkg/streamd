package objectstore

import (
	"context"
	"sync"

	"github.com/nextpkg/streamd/internal/logging"
)

func (client *Client) Retain() func() {
	if client == nil {
		return func() {}
	}
	for {
		client.inspectionMu.Lock()
		if !client.inspectionClosed {
			client.owners++
			client.inspectionMu.Unlock()
			break
		}
		state := client.inspection
		client.inspectionMu.Unlock()
		if state != nil {
			state.workers.Wait()
		}
		client.inspectionMu.Lock()
		if client.inspectionClosed && client.inspection == state {
			client.inspection, client.inspectionClosed = nil, false
			client.owners++
			client.inspectionMu.Unlock()
			break
		}
		client.inspectionMu.Unlock()
	}
	return sync.OnceFunc(func() {
		client.inspectionMu.Lock()
		client.owners--
		retire := client.retired && client.owners == 0
		var state *inspectionState
		if retire {
			state = client.stopInspectionLocked()
		}
		client.inspectionMu.Unlock()
		if retire {
			client.finishRetirement(state)
		}
	})
}

func (client *Client) freshClient() *Client {
	if client.renew == nil {
		return client
	}
	next, err := client.renew()
	if err != nil {
		logging.Warnf("recording client refresh failed; using retained client: %v", err)
		return client
	}
	return next
}

func (client *Client) NewRecordingUpload(ctx context.Context, key string, resume bool) (*MultipartUpload, func(), error) {
	release, err := client.ClaimRecording(key)
	if err != nil {
		return nil, nil, err
	}
	fresh := client.freshClient()
	finish := sync.OnceFunc(func() {
		if fresh != client {
			fresh.Close()
		}
		release()
	})
	upload, err := fresh.NewMultipartUpload(ctx, key, resume)
	if err != nil {
		finish()
		return nil, nil, err
	}
	return upload, finish, nil
}
