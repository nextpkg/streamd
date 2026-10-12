package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRelayLogValidationRequiresExactSessions(test *testing.T) {
	line := func(level, message string) string {
		body, err := json.Marshal(map[string]string{"level": level, "msg": message})
		if err != nil {
			test.Fatal(err)
		}
		return string(body) + "\n"
	}
	dynamic := line("INFO", "[dynamic/sample]New PUBLISH from 127.0.0.1:1234")
	target := dynamic + dynamic + line("INFO", "[static/sample]New PUBLISH from 127.0.0.1:1235") + line("INFO", "[pull/sample]New PUBLISH from 127.0.0.1:1236")
	source := line("INFO", "[source/sample]New PUBLISH from 127.0.0.1:1237") + line("ERROR", "dial relay pusher: Dial rtmp server failed,host='127.0.0.1:1238': connect: connection refused")
	for _, sample := range []struct {
		body   string
		source bool
		valid  bool
	}{
		{target, false, true}, {source, true, true}, {target + dynamic, false, false},
		{strings.TrimPrefix(target, dynamic), false, false}, {source + "panic: failure\n", true, false},
		{target + line("ERROR", "recording failed"), false, false},
		{source + line("ERROR", "read C0C1: EOF"), true, false},
		{source + line("ERROR", "unexpected media failure: connection refused"), true, false},
	} {
		if _, err := validateRelayServiceLog(sample.body, sample.source); (err == nil) != sample.valid {
			test.Errorf("log valid=%v, got err=%v for %s", sample.valid, err, sample.body)
		}
	}
}
