package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nextpkg/streamd/internal/logging"
)

const maxQueue = 4096

type relayClient interface {
	StartPlay() error
	StartPublish() error
	Read(*ChunkStream) error
	Write(*ChunkStream) error
	Close() error
}

type relaySession struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	queue    chan *ChunkStream
	mu       sync.Mutex
	puller   relayClient
	pusher   relayClient
	stopOnce sync.Once
}

func (session *relaySession) attach(client relayClient, pull bool) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.ctx.Err(); err != nil {
		_ = client.Close()
		return err
	}
	if pull {
		session.puller = client
	} else {
		session.pusher = client
	}
	return nil
}

func (session *relaySession) stop() {
	session.stopOnce.Do(func() {
		session.cancel()
		session.mu.Lock()
		puller, pusher := session.puller, session.pusher
		session.mu.Unlock()
		if puller != nil {
			_ = puller.Close()
		}
		if pusher != nil {
			_ = pusher.Close()
		}
	})
}

// DynamicRelay 动态转播
type DynamicRelay struct {
	mu          sync.Mutex
	startGate   chan struct{}
	session     *relaySession
	idle        chan struct{}
	pullURL     string
	pushURL     string
	dialTimeout time.Duration
	dial        func(context.Context, string, time.Duration) (relayClient, error)
}

// NewDynamicRelay 构造动态转播实例
func NewDynamicRelay(pullURL string, pushURL string, dialTimeout time.Duration) *DynamicRelay {
	idle := make(chan struct{})
	close(idle)
	return &DynamicRelay{
		idle: idle, pullURL: pullURL, pushURL: pushURL, dialTimeout: dialTimeout,
		startGate: make(chan struct{}, 1),
		dial: func(ctx context.Context, location string, timeout time.Duration) (relayClient, error) {
			return newConnClientByURL(ctx, location, timeout)
		},
	}
}

// RenewConnection 停止当前会话并等待退出；后续 Start 重新建立连接。
func (relay *DynamicRelay) RenewConnection() {
	relay.mu.Lock()
	session := relay.session
	relay.mu.Unlock()
	if session != nil {
		session.stop()
		<-session.done
	}
}

// Start 启动转播
func (relay *DynamicRelay) Start() error {
	return relay.StartContext(context.Background())
}

func (relay *DynamicRelay) StartContext(startupContext context.Context) (result error) {
	select {
	case relay.startGate <- struct{}{}:
	case <-startupContext.Done():
		return startupContext.Err()
	}
	defer func() { <-relay.startGate }()
	if err := startupContext.Err(); err != nil {
		return err
	}
	relay.mu.Lock()
	if relay.session != nil {
		select {
		case <-relay.session.done:
		default:
			if relay.session.ctx.Err() == nil {
				relay.mu.Unlock()
				return nil
			}
			previous := relay.session.done
			relay.mu.Unlock()
			<-previous
			relay.mu.Lock()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	session := &relaySession{ctx: ctx, cancel: cancel, done: make(chan struct{}), queue: make(chan *ChunkStream, maxQueue)}
	relay.session = session
	relay.mu.Unlock()
	defer func() {
		if result != nil {
			session.stop()
			close(session.done)
			if err := startupContext.Err(); err != nil {
				result = err
			}
		}
	}()
	cancelled := make(chan struct{})
	stopStartup := context.AfterFunc(startupContext, func() {
		session.stop()
		close(cancelled)
	})
	defer func() {
		if stopStartup != nil && !stopStartup() {
			<-cancelled
		}
	}()
	timeout := relay.dialTimeout
	if timeout <= 0 {
		timeout = defaultOption.timeout
	}
	startupTimer := time.AfterFunc(timeout, session.stop)
	defer startupTimer.Stop()
	puller, err := relay.dial(ctx, relay.pullURL, timeout)
	if err != nil {
		return fmt.Errorf("dial relay puller: %w", err)
	}
	if err := session.attach(puller, true); err != nil {
		return err
	}
	if err := puller.StartPlay(); err != nil {
		return fmt.Errorf("start relay puller: %w", err)
	}
	pusher, err := relay.dial(ctx, relay.pushURL, timeout)
	if err != nil {
		return fmt.Errorf("dial relay pusher: %w", err)
	}
	if err := session.attach(pusher, false); err != nil {
		return err
	}
	if err := pusher.StartPublish(); err != nil {
		return fmt.Errorf("start relay pusher: %w", err)
	}
	if !startupTimer.Stop() {
		return context.DeadlineExceeded
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !stopStartup() {
		stopStartup = nil
		<-cancelled
		return startupContext.Err()
	}
	stopStartup = nil
	go func() {
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			defer session.stop()
			relay.pull(session, puller)
		}()
		go func() {
			defer workers.Done()
			defer session.stop()
			relay.push(session, pusher)
		}()
		workers.Wait()
		close(session.done)
	}()
	logging.Infof("Relay (%s) ===> (%s) is turned on", relay.pullURL, relay.pushURL)
	return nil
}

// IsDone 判断当前转播会话是否已结束；尚未启动时也返回 true。
func (relay *DynamicRelay) IsDone() bool {
	select {
	case <-relay.Wait():
		return true
	default:
		return false
	}
}

// Wait 返回当前会话的完成通道。
func (relay *DynamicRelay) Wait() <-chan struct{} {
	relay.mu.Lock()
	defer relay.mu.Unlock()
	if relay.session == nil {
		return relay.idle
	}
	return relay.session.done
}

// Stop 取消当前转播会话并关闭连接。
func (relay *DynamicRelay) Stop() {
	relay.mu.Lock()
	session := relay.session
	relay.mu.Unlock()
	if session != nil {
		session.stop()
	}
}

func (relay *DynamicRelay) pull(session *relaySession, puller relayClient) {
	for session.ctx.Err() == nil {
		chunk := &ChunkStream{}
		if err := puller.Read(chunk); err != nil {
			logging.Tracef("err=%s,pull URL=%s", err, relay.pullURL)
			return
		}
		select {
		case <-session.ctx.Done():
			return
		case session.queue <- chunk:
		default:
			logging.Errorf("packet queue saturated,puller='%s'", relay.pullURL)
			return
		}
	}
}

func (relay *DynamicRelay) push(session *relaySession, pusher relayClient) {
	for {
		select {
		case <-session.ctx.Done():
			return
		case chunk := <-session.queue:
			if err := pusher.Write(chunk); err != nil {
				logging.Tracef("err=%s,push URL=%s", err, relay.pushURL)
				return
			}
		}
	}
}
