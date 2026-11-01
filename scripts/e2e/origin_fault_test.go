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

func TestOriginProxyFaultsAndRecovery(test *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	upstreamDone := make(chan struct{})
	go func() {
		defer close(upstreamDone)
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		_, _ = io.Copy(connection, connection)
	}()
	proxy, err := newOriginProxy(context.Background(), listener.Addr().String(), false)
	if err != nil {
		test.Fatal(err)
	}
	defer proxy.close()
	dial := func() net.Conn {
		test.Helper()
		connection, err := net.DialTimeout("tcp", proxy.listener.Addr().String(), time.Second)
		if err != nil {
			test.Fatal(err)
		}
		test.Cleanup(func() { _ = connection.Close() })
		return connection
	}
	proxy.fault.Store(originProxyReject)
	connection := dial()
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	buffer := make([]byte, 4)
	if count, err := connection.Read(buffer); count != 0 || err == nil {
		test.Fatal("reject proxy passed payload")
	}
	proxy.fault.Store(originProxyStall)
	connection = dial()
	_, _ = connection.Write([]byte("held"))
	_ = connection.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := connection.Read(buffer); err == nil {
		test.Fatal("stalled handshake made progress")
	}
	_ = connection.Close()
	proxy.fault.Store(originProxyPass)
	connection = dial()
	_ = connection.SetDeadline(time.Now().Add(time.Second))
	_, _ = connection.Write([]byte("live"))
	if _, err := io.ReadFull(connection, buffer); err != nil || string(buffer) != "live" {
		test.Fatalf("repaired proxy changed payload %q %v", buffer, err)
	}
	proxy.interrupt()
	if _, err := connection.Read(buffer); err == nil {
		test.Fatal("injected disconnect did not close viewer")
	}
	proxy.close()
	if proxy.accepted.Load() != 3 || proxy.active.Load() != 0 {
		test.Fatalf("proxy accounting accepted=%d active=%d", proxy.accepted.Load(), proxy.active.Load())
	}
	select {
	case <-upstreamDone:
	case <-time.After(time.Second):
		test.Fatal("injected disconnect leaked upstream")
	}
}

func TestInterruptedOriginValidatorRejectsUnrelatedErrors(test *testing.T) {
	const expected = "[in#0/flv @ 0x123abc] Error during demuxing: Input/output error\n[in#0/flv @ 0x123abc] Task finished with error code: -5 (Input/output error)\n[in#0/flv @ 0x123abc] Terminating thread with return code -5 (Input/output error)\n"
	for _, sample := range []struct {
		code  int
		body  string
		valid bool
	}{
		{0, "", true}, {251, expected, true}, {0, expected, false}, {251, "", false}, {1, expected, false}, {251, expected + "WARNING: DATA RACE\n", false},
		{251, strings.ReplaceAll(expected, "Input/output error", "Invalid data found"), false}, {251, strings.ReplaceAll(expected, "in#0/flv", "decoder"), false},
	} {
		if err := validateInterruptedOrigin(sample.code, sample.body); (err == nil) != sample.valid {
			test.Errorf("code=%d valid=%v got %v", sample.code, sample.valid, err)
		}
	}
}

func TestOriginFaultLogValidatorRequiresExactLifecycle(test *testing.T) {
	line := func(level, message string) string {
		body, err := json.Marshal(map[string]string{"level": level, "msg": message})
		if err != nil {
			test.Fatal(err)
		}
		return string(body) + "\n"
	}
	play := line("INFO", "[origin/sample]New PLAY from 127.0.0.1:1")
	attached := line("INFO", "[origin/sample]Origin attached from rtmp://127.0.0.1/origin/sample")
	terminated := line("INFO", "[origin/sample]Client terminated")
	source := line("INFO", "[origin/sample]New PUBLISH from 127.0.0.1:1") + strings.Repeat(play, 2) + terminated
	edge := strings.Repeat(play, 4) + strings.Repeat(attached, 2) + strings.Repeat(terminated, 2)
	for _, sample := range []struct {
		body        string
		edge, valid bool
	}{
		{source, false, true}, {edge, true, true}, {source + play, false, false}, {edge + attached, true, false}, {strings.TrimSuffix(edge, terminated), true, false}, {edge + line("ERROR", "unexpected"), true, false}, {edge + "panic: failure", true, false},
	} {
		if _, err := validateOriginFaultLog(sample.body, sample.edge); (err == nil) != sample.valid {
			test.Errorf("edge=%v valid=%v err=%v", sample.edge, sample.valid, err)
		}
	}
}
