package main

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type originProxy struct {
	listener      net.Listener
	upstream      string
	ctx           context.Context
	cancel        context.CancelFunc
	release       chan struct{}
	releaseOnce   sync.Once
	stopOnce      sync.Once
	workers       sync.WaitGroup
	connections   sync.Map
	upstreamPeers sync.Map
	accepted      atomic.Int64
	firstAccept   atomic.Int64
	active        atomic.Int64
	fault         atomic.Uint32
}

const (
	originProxyPass uint32 = iota
	originProxyReject
	originProxyStall
)

func newOriginProxy(ctx context.Context, upstream string, blocked bool) (*originProxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	proxy := &originProxy{listener: listener, upstream: upstream, ctx: ctx, cancel: cancel, release: make(chan struct{})}
	if !blocked {
		proxy.open()
	}
	proxy.workers.Add(1)
	go func() {
		defer proxy.workers.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			if proxy.accepted.Add(1) == 1 {
				proxy.firstAccept.Store(time.Now().UnixNano())
			}
			proxy.active.Add(1)
			proxy.connections.Store(connection, true)
			proxy.workers.Add(1)
			go proxy.forward(connection)
		}
	}()
	return proxy, nil
}

func (proxy *originProxy) open() { proxy.releaseOnce.Do(func() { close(proxy.release) }) }

func (proxy *originProxy) forward(downstream net.Conn) {
	defer proxy.workers.Done()
	defer proxy.active.Add(-1)
	defer proxy.connections.Delete(downstream)
	defer func() { _ = downstream.Close() }()
	switch proxy.fault.Load() {
	case originProxyReject:
		return
	case originProxyStall:
		_, _ = io.Copy(io.Discard, downstream)
		return
	}
	select {
	case <-proxy.ctx.Done():
		return
	case <-proxy.release:
	}
	dialer := net.Dialer{Timeout: 2 * time.Second}
	upstream, err := dialer.DialContext(proxy.ctx, "tcp", proxy.upstream)
	if err != nil {
		return
	}
	defer func() { _ = upstream.Close() }()
	proxy.upstreamPeers.Store(upstream.LocalAddr().String(), true)
	stop := context.AfterFunc(proxy.ctx, func() { _ = upstream.Close(); _ = downstream.Close() })
	defer stop()
	finished := make(chan struct{})
	go func() {
		_, _ = io.Copy(upstream, downstream)
		_ = upstream.Close()
		_ = downstream.Close()
		close(finished)
	}()
	_, _ = io.Copy(downstream, upstream)
	_ = upstream.Close()
	_ = downstream.Close()
	<-finished
}

func (proxy *originProxy) interrupt() {
	proxy.connections.Range(func(connection, _ any) bool { _ = connection.(net.Conn).Close(); return true })
}

func (proxy *originProxy) close() {
	proxy.stopOnce.Do(func() {
		proxy.cancel()
		_ = proxy.listener.Close()
		proxy.interrupt()
		proxy.workers.Wait()
	})
}
