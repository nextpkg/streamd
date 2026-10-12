package httpx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const callbackCapacity = 32
const callbackResponseLimit = 64 * 1024

var callbacks = newCallbackExecutor()

type callbackFailure struct {
	stage string
	cause error
}

func (failure *callbackFailure) Error() string { return "callback " + failure.stage + " failed" }
func (failure *callbackFailure) Unwrap() error { return failure.cause }

type callbackExecutor struct {
	mu     sync.Mutex
	closed bool
	active int
	wait   sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	client *http.Client
}

func newCallbackExecutor() *callbackExecutor {
	ctx, cancel := context.WithCancel(context.Background())
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = callbackCapacity
	transport.MaxIdleConnsPerHost = callbackCapacity
	return &callbackExecutor{ctx: ctx, cancel: cancel, client: &http.Client{Transport: transport, Timeout: 3 * time.Second}}
}

func SubmitJSON(address string, data []byte, failed func(error)) error {
	return callbacks.submit(address, data, failed)
}

func CloseCallbacks() { callbacks.close() }

func (executor *callbackExecutor) submit(address string, data []byte, failed func(error)) error {
	executor.mu.Lock()
	if executor.closed {
		executor.mu.Unlock()
		return errors.New("callback executor closed")
	}
	if executor.active == callbackCapacity {
		executor.mu.Unlock()
		return errors.New("callback concurrency capacity exceeded")
	}
	executor.active++
	executor.wait.Add(1)
	executor.mu.Unlock()
	body := bytes.Clone(data)
	go func() {
		defer executor.wait.Done()
		defer func() {
			executor.mu.Lock()
			executor.active--
			executor.mu.Unlock()
		}()
		if err := executor.post(address, body); err != nil && failed != nil {
			failed(err)
		}
	}()
	return nil
}

func (executor *callbackExecutor) post(address string, data []byte) error {
	request, err := http.NewRequestWithContext(executor.ctx, http.MethodPost, address, bytes.NewReader(data))
	if err != nil {
		return &callbackFailure{stage: "request", cause: err}
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := executor.client.Do(request)
	if err != nil {
		return &callbackFailure{stage: "request", cause: err}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("callback returned HTTP %d", response.StatusCode)
	}
	size, err := io.Copy(io.Discard, io.LimitReader(response.Body, callbackResponseLimit+1))
	if err != nil {
		return &callbackFailure{stage: "response", cause: err}
	}
	if size > callbackResponseLimit {
		return errors.New("callback response exceeds size limit")
	}
	return nil
}

func (executor *callbackExecutor) close() {
	executor.mu.Lock()
	executor.closed = true
	executor.cancel()
	executor.mu.Unlock()
	executor.wait.Wait()
	executor.client.CloseIdleConnections()
}
