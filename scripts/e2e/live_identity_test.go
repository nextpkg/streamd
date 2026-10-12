package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func liveIdentityFixture(test *testing.T) (recordingIdentity, recordingIdentity, encodedMediaIdentity, encodedMediaIdentity) {
	test.Helper()
	_, source, _ := continuousMediaFixture(test, false)
	body := []byte(`{"streams":[{"codec_type":"video","nb_read_frames":"150"},{"codec_type":"audio","nb_read_frames":"302"}],"format":{"duration":"6.08"}}`)
	if err := json.Unmarshal(body, &source.Media); err != nil {
		test.Fatal(err)
	}
	body, err := json.Marshal(source)
	if err != nil {
		test.Fatal(err)
	}
	var capture recordingIdentity
	if err := json.Unmarshal(body, &capture); err != nil {
		test.Fatal(err)
	}
	capture.Audio, capture.Tracks["audio"] = capture.Audio[2:], capture.Tracks["audio"][2:]
	capture.Media.Streams[1].Frames = "300"
	sourceEncoded := encodedMediaIdentity{Tracks: map[string][]string{"video": append([]string{}, source.Video...), "audio": append([]string{}, source.Audio...)}, Config: map[string]string{"video": "video-config", "audio": "audio-config"}}
	encoded := encodedMediaIdentity{Tracks: map[string][]string{"video": append([]string{}, source.Video...), "audio": append([]string{}, source.Audio[2:]...)}, Config: map[string]string{"video": "video-config", "audio": "audio-config"}}
	return capture, source, encoded, sourceEncoded
}

func TestLiveIdentityRetainsCompleteGOPAndNaturalTail(test *testing.T) {
	capture, source, encoded, sourceEncoded := liveIdentityFixture(test)
	result, err := verifyLiveIdentity(capture, source, encoded, sourceEncoded)
	if err != nil || result.VideoFrames != 150 || result.AudioPackets != 300 || result.MaximumAudioErrorUS != 0 {
		test.Fatalf("complete source rejected: %+v: %v", result, err)
	}
}

func TestLiveIdentityRejectsEncodedAndDecodedCorruption(test *testing.T) {
	for _, sample := range []struct {
		name string
		edit func(*recordingIdentity, *encodedMediaIdentity)
	}{
		{"changed NAL payload", func(capture *recordingIdentity, encoded *encodedMediaIdentity) {
			encoded.Tracks["video"][10] = "changed"
		}},
		{"changed AAC payload", func(capture *recordingIdentity, encoded *encodedMediaIdentity) {
			encoded.Tracks["audio"][10] = "changed"
		}},
		{"changed sequence header", func(capture *recordingIdentity, encoded *encodedMediaIdentity) { encoded.Config["video"] = "changed" }},
		{"lost source origin", func(capture *recordingIdentity, encoded *encodedMediaIdentity) {
			capture.Video, capture.Tracks["video"], encoded.Tracks["video"] = capture.Video[50:], capture.Tracks["video"][50:], encoded.Tracks["video"][50:]
		}},
		{"lost source tail", func(capture *recordingIdentity, encoded *encodedMediaIdentity) {
			capture.Audio, capture.Tracks["audio"], encoded.Tracks["audio"] = capture.Audio[:299], capture.Tracks["audio"][:299], encoded.Tracks["audio"][:299]
		}},
		{"decoded video changed", func(capture *recordingIdentity, encoded *encodedMediaIdentity) { capture.Video[10] = "changed" }},
		{"clock rebased", func(capture *recordingIdentity, encoded *encodedMediaIdentity) {
			for track := range capture.Tracks {
				for index := range capture.Tracks[track] {
					capture.Tracks[track][index].DTS += 23000
					capture.Tracks[track][index].PTS += 23000
				}
			}
		}},
		{"B frame composition changed", func(capture *recordingIdentity, encoded *encodedMediaIdentity) {
			capture.Tracks["video"][10].PTS += 40000
		}},
		{"audio drift", func(capture *recordingIdentity, encoded *encodedMediaIdentity) {
			capture.Tracks["audio"][10].DTS += 20000
		}},
	} {
		test.Run(sample.name, func(test *testing.T) {
			capture, source, encoded, sourceEncoded := liveIdentityFixture(test)
			sample.edit(&capture, &encoded)
			if _, err := verifyLiveIdentity(capture, source, encoded, sourceEncoded); err == nil {
				test.Fatal("accepted incomplete or changed live media")
			}
		})
	}
}

func TestEncodedMediaRequiresValidHashesAndBothTracks(test *testing.T) {
	hash := "SHA256:" + strings.Repeat("12", 32)
	valid := `{"streams":[{"codec_type":"video","extradata_hash":"` + hash + `"},{"codec_type":"audio","extradata_hash":"` + hash + `"}],"packets":[{"codec_type":"video","data_hash":"` + hash + `"},{"codec_type":"audio","data_hash":"` + hash + `"}]}`
	if _, err := parseEncodedMedia([]byte(valid)); err != nil {
		test.Fatal(err)
	}
	for _, invalid := range []string{
		strings.Replace(valid, hash, "MD5:"+strings.Repeat("12", 32), 1),
		strings.Replace(valid, hash, "SHA256:"+strings.Repeat("zz", 32), 1),
		strings.Replace(valid, `"data_hash":"`+hash+`"`, `"data_hash":""`, 1),
		strings.ReplaceAll(valid, `"audio"`, `"video"`),
		strings.ReplaceAll(valid, `"audio"`, `"subtitle"`),
		`{"packets":[]}`,
		`not JSON`,
	} {
		if _, err := parseEncodedMedia([]byte(invalid)); err == nil {
			test.Fatal("accepted malformed or incomplete encoded probe")
		}
	}
}
