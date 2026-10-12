package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestStabilityRecordingFrames(t *testing.T) {
	var packets []stabilityVideoPacket
	for loop := int64(0); ; loop++ {
		finished := false
		for frame := int64(0); frame < 150; frame++ {
			dts := loop*6037 + frame*40
			if len(packets) == 179893 {
				finished = true
				break
			}
			flags := "__"
			if frame%50 == 0 {
				flags = "K_"
			}
			packets = append(packets, stabilityVideoPacket{DTS: &dts, Flags: flags})
		}
		if finished {
			break
		}
	}
	frames, err := stabilityRecordingFrames(packets)
	if err != nil || len(frames) != 1799 || frames[len(frames)-1] != 93 {
		t.Fatalf("looped source plan: segments=%d error=%v", len(frames), err)
	}
	for _, count := range frames[:len(frames)-1] {
		if count != 100 {
			t.Fatalf("unexpected segment frame count %d", count)
		}
	}
	for _, invalid := range [][]stabilityVideoPacket{nil, {{Flags: "K_"}}, {{DTS: packets[0].DTS, Flags: "__"}}, {packets[0], packets[0]}, {packets[1], packets[0]}} {
		if _, err := stabilityRecordingFrames(invalid); err == nil {
			t.Fatal("invalid source accepted")
		}
	}
	boundary := int64(4000)
	minimal, err := stabilityRecordingFrames([]stabilityVideoPacket{packets[0], {DTS: &boundary, Flags: "K_"}})
	if err != nil || !reflect.DeepEqual(minimal, []int{1, 1}) {
		t.Fatalf("exact boundary lost: %v %v", minimal, err)
	}
}

func TestVerifyStabilityRecordingFrames(t *testing.T) {
	fixture := `[ {"file":"first.flv","returncode":0,"media":{"streams":[{"codec_type":"video","nb_read_frames":"100"},{"codec_type":"audio","nb_read_frames":"174"}],"format":{"duration":"4.117"}}}, {"file":"tail.flv","returncode":0,"media":{"streams":[{"codec_type":"video","nb_read_frames":"93"},{"codec_type":"audio","nb_read_frames":"160"}],"format":{"duration":"3.8"}}} ]`
	var recordings []recordingResult
	if err := json.Unmarshal([]byte(fixture), &recordings); err != nil {
		t.Fatal(err)
	}
	expected := []int{100, 93}
	if err := verifyStabilityRecordingFrames(recordings, expected); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(){
		func() { recordings = recordings[:1] },
		func() { recordings = append(recordings, recordings[1]) },
		func() { recordings[1].File = recordings[0].File },
		func() { recordings[1].Media.Streams[0].Frames = "92" },
		func() { recordings[1].Media.Streams[0].Frames = "94" },
		func() { recordings[1].Media.Streams[1].Frames = "0" },
		func() { recordings[1].Media.Format.Duration = "4.6" },
		func() { recordings[1].Media = nil },
		func() { recordings[1].ReturnCode = 1 },
	} {
		recordings = nil
		if err := json.Unmarshal([]byte(fixture), &recordings); err != nil {
			t.Fatal(err)
		}
		mutate()
		if err := verifyStabilityRecordingFrames(recordings, expected); err == nil {
			t.Fatal("damaged recording inventory accepted")
		}
	}
}
