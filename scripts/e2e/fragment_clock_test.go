package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

func continuousMediaFixture(test *testing.T, reset bool) ([]recordingIdentity, recordingIdentity, int64) {
	test.Helper()
	source := recordingIdentity{Tracks: map[string][]recordingPacket{}}
	for index := range 150 {
		source.Video = append(source.Video, fmt.Sprintf("video-%d", index))
		source.Tracks["video"] = append(source.Tracks["video"], recordingPacket{DTS: 23000 + int64(index)*40000, PTS: 103000 + int64(index)*40000, Key: index%50 == 0})
	}
	for index := range 302 {
		source.Audio = append(source.Audio, fmt.Sprintf("audio-%d", index))
		source.Tracks["audio"] = append(source.Tracks["audio"], recordingPacket{DTS: int64(index) * 20000, PTS: int64(index) * 20000, Key: true})
	}
	segment := int64(2000000)
	if reset {
		segment = 4000000
	}
	var files []recordingIdentity
	for start := int64(23000); start < 6000000; start += segment {
		file := recordingIdentity{File: fmt.Sprintf("segment-%d", start), Tracks: map[string][]recordingPacket{}}
		for _, track := range []string{"video", "audio"} {
			for index, packet := range source.Tracks[track] {
				minimum := start
				if !reset && len(files) == 0 {
					minimum = 0
				}
				if packet.DTS < minimum || packet.DTS >= start+segment {
					continue
				}
				if reset {
					packet.DTS -= start
					packet.PTS -= start
				}
				file.Tracks[track] = append(file.Tracks[track], packet)
				if track == "video" {
					file.Video = append(file.Video, source.Video[index])
				} else {
					file.Audio = append(file.Audio, source.Audio[index])
				}
			}
		}
		body := fmt.Sprintf(`{"streams":[{"codec_type":"video","nb_read_frames":"%d"},{"codec_type":"audio","nb_read_frames":"%d"}],"format":{"duration":"%.6f"}}`, len(file.Video), len(file.Audio), float64(len(file.Video))*0.04)
		if err := json.Unmarshal([]byte(body), &file.Media); err != nil {
			test.Fatal(err)
		}
		files = append(files, file)
	}
	return files, source, segment
}

func TestContinuousMediaPreservesAllSourcePacketsAndClocks(test *testing.T) {
	for _, reset := range []bool{false, true} {
		files, source, segment := continuousMediaFixture(test, reset)
		results, err := verifyContinuousMedia(files, source, segment, 1, reset)
		if err != nil || len(results) != len(files) {
			test.Fatalf("reset=%v: %v", reset, err)
		}
		video, audio := 0, 0
		for _, result := range results {
			video += result.VideoFrames
			audio += result.AudioPackets
		}
		preroll := 0
		if reset {
			preroll = 2
		}
		if video != len(source.Video) || audio != len(source.Audio)-preroll {
			test.Fatal("incomplete source coverage or preroll not accounted for")
		}
	}
}

func TestContinuousMediaRejectsMissingOrChangedSourceData(test *testing.T) {
	for _, sample := range []struct {
		name string
		edit func([]recordingIdentity) []recordingIdentity
	}{
		{"missing tail", func(files []recordingIdentity) []recordingIdentity { return files[:len(files)-1] }},
		{"missing start", func(files []recordingIdentity) []recordingIdentity { return files[1:] }},
		{"reordered files", func(files []recordingIdentity) []recordingIdentity {
			files[0], files[1] = files[1], files[0]
			return files
		}},
		{"changed video", func(files []recordingIdentity) []recordingIdentity { files[0].Video[1] = "corrupt"; return files }},
		{"repeated audio", func(files []recordingIdentity) []recordingIdentity {
			files[0].Audio[1] = files[0].Audio[0]
			return files
		}},
		{"video DTS", func(files []recordingIdentity) []recordingIdentity {
			files[0].Tracks["video"][1].DTS += 2
			return files
		}},
		{"video PTS", func(files []recordingIdentity) []recordingIdentity {
			files[0].Tracks["video"][1].PTS += 40000
			return files
		}},
		{"audio clock", func(files []recordingIdentity) []recordingIdentity {
			files[0].Tracks["audio"][1].DTS += 20000
			return files
		}},
		{"not keyframe", func(files []recordingIdentity) []recordingIdentity {
			files[0].Tracks["video"][0].Key = false
			return files
		}},
		{"clock offset", func(files []recordingIdentity) []recordingIdentity {
			for track := range files[1].Tracks {
				for index := range files[1].Tracks[track] {
					files[1].Tracks[track][index].DTS += 40000
					files[1].Tracks[track][index].PTS += 40000
				}
			}
			return files
		}},
		{"fake duration", func(files []recordingIdentity) []recordingIdentity {
			files[0].Media.Format.Duration = "NaN"
			return files
		}},
	} {
		for _, reset := range []bool{false, true} {
			test.Run(fmt.Sprintf("%s/reset=%v", sample.name, reset), func(test *testing.T) {
				files, source, segment := continuousMediaFixture(test, reset)
				if _, err := verifyContinuousMedia(sample.edit(files), source, segment, 0, reset); err == nil {
					test.Fatal("accepted corrupted continuous media")
				}
			})
		}
	}
	for _, target := range []int64{1000000, 3000000} {
		files, source, _ := continuousMediaFixture(test, false)
		if _, err := verifyContinuousMedia(files, source, target, 1, false); err == nil {
			test.Fatal("accepted skipped or premature segmentation boundary")
		}
	}
}
