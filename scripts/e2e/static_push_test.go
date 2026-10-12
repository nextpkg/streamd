package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStaticPushLogRejectsUnexpectedFailuresAndReconnects(test *testing.T) {
	event := func(level, message string) string {
		body, err := json.Marshal(map[string]string{"level": level, "msg": message})
		if err != nil {
			test.Fatal(err)
		}
		return string(body) + "\n"
	}
	publish := func(app string, count int) string {
		return strings.Repeat(event("INFO", "["+app+"/sample]New PUBLISH from 127.0.0.1:10000"), count)
	}
	timeout := event("ERROR", "start relay pusher: client handshake to server: receive S0S1S2: read failed,read=0: read tcp 127.0.0.1:10000->127.0.0.1:20000: i/o timeout")
	source := publish("origin", 2) + timeout
	destination := publish("control", 1) + publish("healthy", 2) + publish("unstable", 3) + publish("stalled", 1)
	for _, sample := range []struct {
		body   string
		source bool
	}{{source, true}, {destination, false}} {
		counts, err := validateStaticPushLog(sample.body+event("WARN", "Not continuous timestamp"), sample.source)
		if err != nil || counts["retained_timestamp_warnings"] != 1 {
			test.Fatalf("valid static-push evidence rejected or warning hidden: counts=%v error=%v", counts, err)
		}
	}
	for name, sample := range map[string]struct {
		body   string
		source bool
	}{
		"missing-injected-timeout":   {publish("origin", 2), true},
		"extra-timeout":              {source + timeout, true},
		"failed-owner-cancellation":  {source + event("ERROR", "context canceled"), true},
		"healthy-target-reconnected": {destination + publish("healthy", 1), false},
		"target-not-recovered":       {strings.Replace(destination, publish("unstable", 1), "", 1), false},
		"destination-error":          {destination + timeout, false},
		"recording-error":            {source + event("ERROR", "recording failed"), true},
		"unknown-warning":            {destination + event("WARN", "packet queue saturated"), false},
	} {
		test.Run(name, func(test *testing.T) {
			if _, err := validateStaticPushLog(sample.body, sample.source); err == nil {
				test.Fatal("unexpected static-push failure accepted")
			}
		})
	}
}
