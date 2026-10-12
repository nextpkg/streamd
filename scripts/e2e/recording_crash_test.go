package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCrashAppendRequiresExactPrefixSourceAndClocks(test *testing.T) {
	prefix := recordingIdentity{Video: []string{"old-v0", "old-v1"}, Audio: []string{"old-a0", "old-a1"}, Tracks: map[string][]recordingPacket{
		"video": {{DTS: 0, PTS: 0, Key: true}, {DTS: 40000, PTS: 40000}},
		"audio": {{DTS: 0, PTS: 0, Key: true}, {DTS: 23000, PTS: 23000, Key: true}},
	}}
	source := recordingIdentity{Video: []string{"new-v0", "new-v1"}, Audio: []string{"new-a0", "new-a1"}, Tracks: prefix.Tracks}
	valid := recordingIdentity{Video: []string{"old-v0", "old-v1", "new-v0", "new-v1"}, Audio: []string{"old-a0", "old-a1", "new-a0", "new-a1"}, Tracks: map[string][]recordingPacket{}}
	for track, packets := range prefix.Tracks {
		valid.Tracks[track] = append([]recordingPacket(nil), packets...)
		for _, packet := range source.Tracks[track] {
			packet.DTS += 80000
			packet.PTS += 80000
			valid.Tracks[track] = append(valid.Tracks[track], packet)
		}
	}
	if err := verifyCrashAppend(prefix, source, valid, 10000); err != nil {
		test.Fatal(err)
	}
	for _, mutate := range []func(*recordingIdentity){
		func(value *recordingIdentity) { value.Video[0] = "changed prefix" },
		func(value *recordingIdentity) { value.Video[3] = value.Video[2] },
		func(value *recordingIdentity) { value.Audio = value.Audio[:3] },
		func(value *recordingIdentity) { value.Tracks["video"][0].PTS++ },
		func(value *recordingIdentity) { value.Tracks["video"][2].DTS = 0 },
		func(value *recordingIdentity) { value.Tracks["audio"][2].PTS += 11000 },
		func(value *recordingIdentity) { value.Tracks["video"][2].Key = false },
		func(value *recordingIdentity) { value.Tracks["audio"] = nil },
	} {
		encoded, err := json.Marshal(valid)
		if err != nil {
			test.Fatal(err)
		}
		var corrupted recordingIdentity
		if err := json.Unmarshal(encoded, &corrupted); err != nil {
			test.Fatal(err)
		}
		mutate(&corrupted)
		if err := verifyCrashAppend(prefix, source, corrupted, 10000); err == nil {
			test.Fatal("corrupt crash recovery accepted")
		}
	}
}

func TestCrashLogsRejectUnrelatedAndMissingFailures(test *testing.T) {
	var lines []string
	for _, message := range []string{"invalid existing FLV tail length", "existing TS PES length does not match recorded payload", "[flv/sample]recording failed: invalid existing FLV tail length", "[ts/sample]recording failed: existing TS PES length does not match recorded payload", "close failed,sync=false,block=true: invalid existing FLV tail length", "close failed,sync=false,block=true: existing TS PES length does not match recorded payload"} {
		body, err := json.Marshal(map[string]string{"level": "ERROR", "msg": message})
		if err != nil {
			test.Fatal(err)
		}
		lines = append(lines, string(body))
	}
	valid := strings.Join(lines, "\n")
	if _, err := validateCrashServiceLog([]byte(valid), true); err != nil {
		test.Fatal(err)
	}
	if _, err := validateCrashServiceLog([]byte(valid), false); err == nil {
		test.Fatal("injected errors allowed in the healthy crash-resume phase")
	}
	for _, body := range []string{strings.Join(lines[:5], "\n"), valid + "\npanic: failed", valid + "\n" + `{"level":"ERROR","msg":"unrelated storage error"}`, valid + "\n" + `{"level":"WARN","msg":"unexpected warning"}`, valid + "\n" + `{"level":"ERROR","msg":"close failed,sync=true,block=true: invalid existing FLV tail length"}`, valid + "\n" + `{"level":"ERROR","msg":"close failed,sync=false,block=true: unrelated storage error"}`} {
		if _, err := validateCrashServiceLog([]byte(body), true); err == nil {
			test.Fatal("unverified service failure accepted")
		}
	}
}
