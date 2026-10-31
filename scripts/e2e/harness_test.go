package main

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSegmentURL(test *testing.T) {
	for _, sample := range []struct {
		playlist string
		want     string
	}{
		{"#EXTM3U\n#EXTINF:2\nsample/first.ts\n#EXTINF:2\nsample/last.ts?token=abc&e=123\n", "http://127.0.0.1:1234/hls/live/sample/last.ts?token=abc&e=123"},
		{"#EXTM3U\n/hls/live/sample/last.ts\n", "http://127.0.0.1:1234/hls/live/sample/last.ts"},
		{"#EXTM3U\n", ""},
	} {
		actual, err := segmentURL("http://127.0.0.1:1234", "live", sample.playlist)
		if sample.want == "" {
			if err == nil {
				test.Fatal("empty playlist accepted")
			}
		} else if err != nil || actual != sample.want {
			test.Fatalf("got %q, %v; want %q", actual, err, sample.want)
		}
	}
}

func TestSignatureURLs(test *testing.T) {
	invalid, expired, err := signatureURLs("http://127.0.0.1/hls/live/sample/1.ts?token=old&e=1900000000", "live/sample", "test-third-key", 1700000000)
	if err != nil {
		test.Fatal(err)
	}
	for location, expected := range map[string]map[string]string{
		invalid: {"token": "forged", "e": "1900000000"},
		expired: {"token": "CnpD3_vpWZoqfXVTQSqHPg", "e": "1700000000"},
	} {
		parsed, err := url.Parse(location)
		if err != nil {
			test.Fatal(err)
		}
		if parsed.Path != "/hls/live/sample/1.ts" {
			test.Fatalf("signature mutation changed path: %s", location)
		}
		for key, value := range expected {
			if parsed.Query().Get(key) != value {
				test.Fatalf("%s: %s != %q", location, key, value)
			}
		}
	}
	if _, _, err := signatureURLs("http://127.0.0.1/unsigned.ts", "live/sample", "key", 1); err == nil {
		test.Fatal("unsigned URL accepted as signed")
	}
}

func TestMediaProbeValidation(test *testing.T) {
	valid := `{"streams":[{"codec_type":"video","nb_read_frames":"100"},{"codec_type":"audio","nb_read_frames":"170"}],"format":{"duration":"4.08"}}`
	for _, sample := range []struct {
		document string
		valid    bool
	}{
		{valid, true},
		{strings.Replace(valid, "4.08", "4.51", 1), false},
		{strings.Replace(valid, "4.08", "0", 1), false},
		{strings.Replace(valid, "4.08", "NaN", 1), false},
		{strings.Replace(valid, `"100"`, `"0"`, 1), false},
		{strings.Replace(valid, `"170"`, `"N/A"`, 1), false},
		{strings.Replace(valid, `"audio"`, `"video"`, 1), false},
		{`{"streams":[],"format":{"duration":"4"}}`, false},
	} {
		var media mediaProbe
		if err := json.Unmarshal([]byte(sample.document), &media); err != nil {
			test.Fatal(err)
		}
		if err := media.validate(4.5); (err == nil) != sample.valid {
			test.Fatalf("%s: validation=%v", sample.document, err)
		}
	}
}

func TestComponentsApplied(test *testing.T) {
	expected := map[componentKey]string{{"hls", "live/sample"}: "active", {"recording", "live/sample"}: "disabled"}
	report := configurationReport{Revision: 3, Components: []componentStatus{
		{Component: "hls", Stream: "live/sample", Revision: 3, State: "active"},
		{Component: "recording", Stream: "live/sample", Revision: 3, State: "disabled"},
	}}
	if !componentsApplied(report, 3, expected) {
		test.Fatal("applied state rejected")
	}
	if componentsApplied(report, 2, expected) {
		test.Fatal("wrong global revision accepted")
	}
	report.Components[1].Revision = 2
	if componentsApplied(report, 3, expected) {
		test.Fatal("old component revision accepted")
	}
	report.Components[1].Revision, report.Components[1].State = 3, "waiting_keyframe"
	if componentsApplied(report, 3, expected) {
		test.Fatal("pending keyframe accepted")
	}
	report.Components = report.Components[:1]
	if componentsApplied(report, 3, expected) {
		test.Fatal("missing component accepted")
	}
}

func TestVerifyContinuity(test *testing.T) {
	beforeJSON := `{"count":3,"list":[{"pts":100,"duration":10,"videoReceived":1000},{"pts":105,"duration":11,"videoReceived":1100},{"pts":110,"duration":12,"videoReceived":1200}]}`
	afterJSON := `{"count":3,"list":[{"pts":120,"duration":12,"videoReceived":1500},{"pts":125,"duration":13,"videoReceived":1600},{"pts":130,"duration":14,"videoReceived":1700}]}`
	var before subscriberReport
	if err := json.Unmarshal([]byte(beforeJSON), &before); err != nil {
		test.Fatal(err)
	}
	for _, sample := range []struct {
		document string
		valid    bool
	}{
		{afterJSON, true},
		{strings.Replace(afterJSON, `"count":3`, `"count":2`, 1), false},
		{strings.Replace(afterJSON, `"pts":120`, `"pts":110`, 1), false},
		{strings.Replace(afterJSON, `"duration":12`, `"duration":9`, 1), false},
		{strings.Replace(afterJSON, `"videoReceived":1500`, `"videoReceived":999`, 1), false},
		{`{"count":3,"list":[]}`, false},
	} {
		var after subscriberReport
		if err := json.Unmarshal([]byte(sample.document), &after); err != nil {
			test.Fatal(err)
		}
		if err := verifyContinuity(before, after); (err == nil) != sample.valid {
			test.Fatalf("%s: validation=%v", sample.document, err)
		}
	}
}

func TestAtomicSettings(test *testing.T) {
	harness := &harness{ctx: context.Background(), evidence: test.TempDir(), settings: serviceConfig{LogLevel: "INFO"}}
	if err := harness.atomicSettings(); err != nil {
		test.Fatal(err)
	}
	harness.settings.LogLevel = "WARN"
	if err := harness.atomicSettings(); err != nil {
		test.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(harness.evidence, "config.json"))
	if err != nil {
		test.Fatal(err)
	}
	var actual serviceConfig
	if err := json.Unmarshal(data, &actual); err != nil || actual.LogLevel != "WARN" {
		test.Fatalf("invalid replacement: %s: %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(harness.evidence, "candidate.json")); !os.IsNotExist(err) {
		test.Fatalf("candidate not consumed: %v", err)
	}
}

func TestRecordingFilesAndSizes(test *testing.T) {
	harness := &harness{evidence: test.TempDir()}
	directory := filepath.Join(harness.evidence, "recordings-a", "live", "sample")
	if err := os.MkdirAll(directory, 0700); err != nil {
		test.Fatal(err)
	}
	for _, name := range []string{"1.flv", "2.ts", "notes.txt"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("media"), 0600); err != nil {
			test.Fatal(err)
		}
	}
	files, err := recordingFiles(filepath.Join(harness.evidence, "recordings-a"))
	if err != nil || len(files) != 2 {
		test.Fatalf("files=%v error=%v", files, err)
	}
	sizes, err := harness.recordingSizes()
	if err != nil || len(sizes) != 3 {
		test.Fatalf("sizes=%v error=%v", sizes, err)
	}
	for _, size := range sizes {
		if size != 5 {
			test.Fatalf("incorrect size %d", size)
		}
	}
}
