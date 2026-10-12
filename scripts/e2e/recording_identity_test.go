package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
)

func identityFixture(test *testing.T) ([]recordingIdentity, recordingIdentity, recordingIdentity) {
	test.Helper()
	source := func(name string, videoStart, audioStart, composition int64) recordingIdentity {
		identity := recordingIdentity{Tracks: map[string][]recordingPacket{}}
		for index := range 200 {
			stamp := videoStart + int64(index)*40000
			identity.Video = append(identity.Video, fmt.Sprintf("%s-video-%d", name, index))
			identity.Tracks["video"] = append(identity.Tracks["video"], recordingPacket{DTS: stamp, PTS: stamp + composition, Key: index%50 == 0})
		}
		for index := range 345 {
			stamp := audioStart + int64(index)*23220
			identity.Audio = append(identity.Audio, fmt.Sprintf("%s-audio-%d", name, index))
			identity.Tracks["audio"] = append(identity.Tracks["audio"], recordingPacket{DTS: stamp, PTS: stamp})
		}
		return identity
	}
	initial, final := source("old", 23000, 0, 0), source("new", 0, 57000, 80000)
	files := make([]recordingIdentity, 3)
	for index := range files {
		files[index] = recordingIdentity{File: fmt.Sprintf("%d.ts", index), Tracks: map[string][]recordingPacket{}}
	}
	appendPacket := func(track, digest string, packet recordingPacket) {
		index, offset := 0, int64(0)
		if packet.DTS >= 8520000 {
			index, offset = 2, 8520000
		} else if packet.DTS >= 4000000 {
			index, offset = 1, 4000000
		}
		packet.DTS -= offset
		packet.PTS -= offset
		files[index].Tracks[track] = append(files[index].Tracks[track], packet)
		if track == "video" {
			files[index].Video = append(files[index].Video, digest)
		} else {
			files[index].Audio = append(files[index].Audio, digest)
		}
	}
	for _, track := range []string{"video", "audio"} {
		for generation, reference := range []recordingIdentity{initial, final} {
			hashes := reference.Video
			if track == "audio" {
				hashes = reference.Audio
			}
			for index, packet := range reference.Tracks[track] {
				shift := int64(4520000)
				if generation == 0 {
					if packet.DTS < 23000 || packet.DTS >= 4543000 {
						continue
					}
					shift = -23000
				}
				packet.DTS += shift
				packet.PTS += shift
				appendPacket(track, hashes[index], packet)
			}
		}
	}
	for index := range files {
		file := &files[index]
		start, end := int64(math.MaxInt64), int64(0)
		for track, packets := range file.Tracks {
			period := int64(40000)
			if track == "audio" {
				period = 23220
			}
			for _, packet := range packets {
				start = min(start, packet.PTS)
				end = max(end, packet.PTS+period)
			}
		}
		body := fmt.Sprintf(`{"streams":[{"codec_type":"video","nb_read_frames":"%d"},{"codec_type":"audio","nb_read_frames":"%d"}],"format":{"duration":"%.6f"}}`, len(file.Video), len(file.Audio), float64(end-start)/1e6)
		if err := json.Unmarshal([]byte(body), &file.Media); err != nil {
			test.Fatal(err)
		}
	}
	return files, initial, final
}

func TestTakeoverRecordingIdentityAndSourceBoundary(test *testing.T) {
	files, initial, final := identityFixture(test)
	if err := files[1].Media.validate(4.5); err == nil {
		test.Fatal("counterexample must exceed the old fixed duration limit")
	}
	results, err := verifyTakeoverRecordings(files, initial, final, 1)
	if err != nil {
		test.Fatal(err)
	}
	if results[1].NextKeyframeSeconds != 4.52 || !results[1].SourceTransition || results[1].VideoFrames != 113 {
		test.Fatalf("unexpected source boundary: %+v", results[1])
	}
}

func TestTakeoverRecordingRejectsContentAndClockCorruption(test *testing.T) {
	for _, example := range []struct {
		name   string
		mutate func([]recordingIdentity)
	}{
		{"lost video", func(files []recordingIdentity) {
			files[2].Video = files[2].Video[1:]
			files[2].Tracks["video"] = files[2].Tracks["video"][1:]
		}},
		{"duplicate video", func(files []recordingIdentity) { files[1].Video[20] = files[1].Video[19] }},
		{"lost audio", func(files []recordingIdentity) {
			files[2].Audio = files[2].Audio[1:]
			files[2].Tracks["audio"] = files[2].Tracks["audio"][1:]
		}},
		{"changed audio", func(files []recordingIdentity) { files[1].Audio[20] = "corrupt" }},
		{"reordered frame", func(files []recordingIdentity) {
			files[2].Video[2], files[2].Video[3] = files[2].Video[3], files[2].Video[2]
		}},
		{"PTS corruption", func(files []recordingIdentity) { files[2].Tracks["video"][50].PTS += 40000 }},
		{"DTS corruption", func(files []recordingIdentity) { files[2].Tracks["video"][50].DTS += 40000 }},
		{"audio drift", func(files []recordingIdentity) { files[2].Tracks["audio"][50].PTS += 11000 }},
		{"non-keyframe start", func(files []recordingIdentity) { files[2].Tracks["video"][0].Key = false }},
		{"timestamp gap", func(files []recordingIdentity) {
			for index := 13; index < len(files[1].Tracks["video"]); index++ {
				files[1].Tracks["video"][index].DTS += 400000
				files[1].Tracks["video"][index].PTS += 400000
			}
		}},
		{"false container duration", func(files []recordingIdentity) { files[1].Media.Format.Duration = "5.6" }},
		{"non-finite duration", func(files []recordingIdentity) { files[1].Media.Format.Duration = "NaN" }},
	} {
		test.Run(example.name, func(test *testing.T) {
			files, initial, final := identityFixture(test)
			example.mutate(files)
			if _, err := verifyTakeoverRecordings(files, initial, final, 10001); err == nil {
				test.Fatal("accepted corrupted recording")
			}
		})
	}
}

func TestTakeoverRecordingRejectsEarlyAndLateKeyframeCuts(test *testing.T) {
	for _, count := range []int{50, 150} {
		test.Run(strconv.Itoa(count), func(test *testing.T) {
			files, initial, final := identityFixture(test)
			allVideo := append(append([]string{}, files[0].Video...), files[1].Video...)
			allPackets := append([]recordingPacket{}, files[0].Tracks["video"]...)
			for _, packet := range files[1].Tracks["video"] {
				packet.DTS += 4000000
				packet.PTS += 4000000
				allPackets = append(allPackets, packet)
			}
			files[0].Video, files[1].Video = allVideo[:count], allVideo[count:]
			files[0].Tracks["video"], files[1].Tracks["video"] = allPackets[:count], allPackets[count:]
			_, err := verifyTakeoverRecordings(files, initial, final, 1)
			if err == nil || (!strings.Contains(err.Error(), "before its target") && !strings.Contains(err.Error(), "skipped an eligible")) {
				test.Fatalf("did not reject wrong boundary directly: %v", err)
			}
		})
	}
}

func TestFrameDigestParserRejectsInvalidEvidence(test *testing.T) {
	valid := "0, 0, 0, 1, 10, 0123456789abcdef0123456789abcdef"
	for _, row := range []string{valid, valid + ", S=1, 1, 123"} {
		if _, err := frameDigests([]byte(row)); err != nil {
			test.Fatal(err)
		}
	}
	for _, row := range []string{"", "#empty\n", "0,0", strings.Replace(valid, "0123456789abcdef0123456789abcdef", "NaN", 1), valid + ", unexpected"} {
		if _, err := frameDigests([]byte(row)); err == nil {
			test.Fatalf("accepted %q", row)
		}
	}
}
