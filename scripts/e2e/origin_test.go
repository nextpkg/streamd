package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestOriginProxyBlocksUntilReleased(test *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		close(accepted)
		_, _ = io.Copy(connection, connection)
	}()
	proxy, err := newOriginProxy(context.Background(), listener.Addr().String(), true)
	if err != nil {
		test.Fatal(err)
	}
	defer proxy.close()
	connection, err := net.DialTimeout("tcp", proxy.listener.Addr().String(), time.Second)
	if err != nil {
		test.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if _, err := connection.Write([]byte("origin-payload")); err != nil {
		test.Fatal(err)
	}
	select {
	case <-accepted:
		test.Fatal("blocked proxy contacted its upstream")
	case <-time.After(100 * time.Millisecond):
	}
	proxy.open()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		test.Fatal(err)
	}
	data := make([]byte, len("origin-payload"))
	if _, err := io.ReadFull(connection, data); err != nil || string(data) != "origin-payload" {
		test.Fatalf("proxy did not preserve payload: %q: %v", data, err)
	}
	proxy.close()
	if proxy.accepted.Load() != 1 || proxy.active.Load() != 0 {
		test.Fatalf("proxy connection accounting: accepted=%d active=%d", proxy.accepted.Load(), proxy.active.Load())
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		test.Fatal("upstream stayed connected after proxy close")
	}
}

func TestOriginProxyShutdownCancelsBlockedConnections(test *testing.T) {
	proxy, err := newOriginProxy(context.Background(), "127.0.0.1:1", true)
	if err != nil {
		test.Fatal(err)
	}
	defer proxy.close()
	var connections []net.Conn
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	for count := 0; count < 4; count++ {
		connection, err := net.DialTimeout("tcp", proxy.listener.Addr().String(), time.Second)
		if err != nil {
			test.Fatal(err)
		}
		connections = append(connections, connection)
	}
	deadline := time.Now().Add(time.Second)
	for proxy.active.Load() != 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if proxy.active.Load() != 4 {
		test.Fatal("proxy did not accept all blocked clients")
	}
	finished := make(chan struct{})
	go func() { proxy.close(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		test.Fatal("shutdown waited for the upstream gate")
	}
	for _, connection := range connections {
		if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			test.Fatal(err)
		}
		if _, err := connection.Read(make([]byte, 1)); err != io.EOF {
			test.Fatalf("blocked connection not closed: %v", err)
		}
	}
	if proxy.active.Load() != 0 || proxy.accepted.Load() != 4 {
		test.Fatal("shutdown leaked proxy connections")
	}
}

func TestOriginLogValidationRequiresExactLifecycle(test *testing.T) {
	line := func(level, message string) string {
		body, err := json.Marshal(map[string]string{"level": level, "msg": message})
		if err != nil {
			test.Fatal(err)
		}
		return string(body) + "\n"
	}
	play := line("INFO", "[origin/sample]New PLAY from 127.0.0.1:1")
	origin := line("INFO", "[origin/sample]Origin attached from rtmp://127.0.0.1/origin/sample")
	idle := line("INFO", "[origin/sample]Data source idling over 6s.")
	source := line("INFO", "[origin/sample]New PUBLISH from 127.0.0.1:2") + play
	edge := strings.Repeat(play, 19) + origin + origin + idle + idle
	for _, sample := range []struct {
		body  string
		edge  bool
		valid bool
	}{
		{source, false, true}, {edge, true, true}, {edge + origin, true, false},
		{strings.TrimSuffix(edge, idle), true, false}, {source + play, false, false},
		{edge + line("ERROR", "recording failed"), true, false}, {source + "panic: failed\n", false, false},
	} {
		if _, err := validateOriginServiceLog(sample.body, sample.edge); (err == nil) != sample.valid {
			test.Errorf("origin log valid=%v, got %v", sample.valid, err)
		}
	}
}
