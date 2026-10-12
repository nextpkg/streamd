package main

import (
	"strings"
	"testing"
)

func TestShutdownPublisherRequiresExactEPIPEEvents(test *testing.T) {
	valid := "[vost#0:0/copy @ 0xabc123] Error submitting a packet to the muxer: Broken pipe\n" +
		"    Last message repeated 1 times\n" +
		"[out#0/flv @ 0xabc123] Error muxing a packet\n" +
		"[out#0/flv @ 0xabc123] Task finished with error code: -32 (Broken pipe)\n" +
		"[out#0/flv @ 0xabc123] Terminating thread with return code -32 (Broken pipe)\n" +
		"[out#0/flv @ 0xabc123] Error writing trailer: Broken pipe\n" +
		"[out#0/flv @ 0xabc123] Error closing file: Broken pipe\n"
	eofThenPipe := "[vost#0:0/copy @ 0xabc123] Error submitting a packet to the muxer: End of file\n" +
		"[out#0/flv @ 0xabc123] Error writing trailer: End of file\n" +
		"[out#0/flv @ 0xabc123] Error closing file: Broken pipe\n"
	for _, sample := range []struct {
		code     int
		body     string
		accepted bool
	}{
		{224, valid, true}, {224, strings.ReplaceAll(valid, "    Last message repeated 1 times\n", ""), true},
		{224, eofThenPipe, true}, {0, eofThenPipe, false},
		{224, strings.ReplaceAll(eofThenPipe, "Broken pipe", "End of file"), false},
		{224, strings.ReplaceAll(eofThenPipe, "End of file", "Input/output error"), false},
		{224, strings.ReplaceAll(eofThenPipe, "vost#0:0/copy", "out#0/flv"), false},
		{0, valid, false}, {1, valid, false}, {224, "", false},
		{224, valid + "WARNING: DATA RACE\n", false},
		{224, strings.ReplaceAll(valid, "Broken pipe", "Invalid data found"), false},
		{224, strings.ReplaceAll(valid, "out#0/flv", "decoder"), false},
		{224, valid + "[out#0/flv @ 0xabc123] Error closing file: Broken pipe\n", false},
		{224, strings.ReplaceAll(valid, "[out#0/flv @ 0xabc123] Error writing trailer: Broken pipe\n", ""), false},
	} {
		if err := validateShutdownPublisher(sample.code, sample.body); (err == nil) != sample.accepted {
			test.Fatalf("exit=%d body=%q accepted=%t error=%v", sample.code, sample.body, sample.accepted, err)
		}
	}
}

func TestShutdownPublisherRequiresExactResetThenPipeEvents(test *testing.T) {
	valid := "[aost#0:1/copy @ 0xabc123] Error submitting a packet to the muxer: Connection reset by peer\n" +
		"    Last message repeated 1 times\n" +
		"[out#0/flv @ 0xabc123] Error muxing a packet\n" +
		"[out#0/flv @ 0xabc123] Task finished with error code: -54 (Connection reset by peer)\n" +
		"[out#0/flv @ 0xabc123] Terminating thread with return code -54 (Connection reset by peer)\n" +
		"[out#0/flv @ 0xabc123] Error writing trailer: Connection reset by peer\n" +
		"[out#0/flv @ 0xabc123] Error closing file: Broken pipe\n"
	for _, sample := range []struct {
		code     int
		body     string
		accepted bool
	}{
		{202, valid, true}, {202, strings.ReplaceAll(valid, "    Last message repeated 1 times\n", ""), true},
		{224, valid, false}, {0, valid, false}, {202, valid + "WARNING: DATA RACE\n", false},
		{202, strings.ReplaceAll(valid, "-54", "-32"), false},
		{202, strings.ReplaceAll(valid, "Connection reset by peer", "Input/output error"), false},
		{202, strings.ReplaceAll(valid, "Error closing file: Broken pipe", "Error closing file: Connection reset by peer"), false},
		{202, strings.ReplaceAll(valid, "[out#0/flv @ 0xabc123] Error writing trailer: Connection reset by peer\n", ""), false},
	} {
		if err := validateShutdownPublisher(sample.code, sample.body); (err == nil) != sample.accepted {
			test.Fatalf("reset exit=%d accepted=%t error=%v", sample.code, sample.accepted, err)
		}
	}
}
