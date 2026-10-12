package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestResumedMediaRequiresExactFramesAndContinuousDTS(test *testing.T) {
	valid := `{"streams":[{"codec_type":"video","nb_read_frames":"2"},{"codec_type":"audio","nb_read_frames":"2"}],"format":{"duration":"1.04"},"packets":[{"codec_type":"video","dts_time":"1.00"},{"codec_type":"audio","dts_time":"1.00"},{"codec_type":"video","dts_time":"1.04"},{"codec_type":"audio","dts_time":"1.023"}]}`
	for _, sample := range []struct {
		document string
		valid    bool
	}{
		{valid, true},
		{strings.Replace(valid, `"nb_read_frames":"2"`, `"nb_read_frames":"1"`, 1), false},
		{strings.Replace(valid, `"dts_time":"1.04"`, `"dts_time":"0.00"`, 1), false},
		{strings.Replace(valid, `"dts_time":"1.04"`, `"dts_time":"1.50"`, 1), false},
		{strings.Replace(valid, `"dts_time":"1.04"`, `"dts_time":""`, 1), false},
		{strings.Replace(valid, `"duration":"1.04"`, `"duration":"0.50"`, 1), false},
	} {
		var probe packetProbe
		if err := json.Unmarshal([]byte(sample.document), &probe); err != nil {
			test.Fatal(err)
		}
		if err := verifyResumedMedia(probe, map[string]int{"video": 2, "audio": 2}, 1.04); (err == nil) != sample.valid {
			test.Fatalf("%s: validation=%v", sample.document, err)
		}
	}
}
