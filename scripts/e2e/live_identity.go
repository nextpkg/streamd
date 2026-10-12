package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

type encodedMediaIdentity struct {
	Tracks map[string][]string `json:"tracks"`
	Config map[string]string   `json:"codec_configuration"`
}

func inspectEncodedMedia(ctx context.Context, filename string) (encodedMediaIdentity, error) {
	body, err := mediaCommand(ctx, "ffprobe", "-v", "error", "-fflags", "+nofillin", "-show_packets", "-show_streams", "-show_data_hash", "sha256", "-show_entries", "packet=codec_type,data_hash:stream=codec_type,extradata_hash", "-of", "json", filename)
	if err != nil {
		return encodedMediaIdentity{}, err
	}
	return parseEncodedMedia(body)
}

func parseEncodedMedia(body []byte) (encodedMediaIdentity, error) {
	result := encodedMediaIdentity{Tracks: map[string][]string{}, Config: map[string]string{}}
	validHash := func(value string) bool {
		digest, found := strings.CutPrefix(value, "SHA256:")
		decoded, err := hex.DecodeString(digest)
		return found && err == nil && len(decoded) == 32
	}
	var probe struct {
		Packets []struct {
			Type string `json:"codec_type"`
			Hash string `json:"data_hash"`
		} `json:"packets"`
		Streams []struct {
			Type string `json:"codec_type"`
			Hash string `json:"extradata_hash"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return result, err
	}
	for _, stream := range probe.Streams {
		if (stream.Type != "video" && stream.Type != "audio") || !validHash(stream.Hash) || result.Config[stream.Type] != "" {
			return result, errors.New("missing or duplicate encoded media configuration")
		}
		result.Config[stream.Type] = stream.Hash
	}
	for _, packet := range probe.Packets {
		if result.Config[packet.Type] == "" || !validHash(packet.Hash) {
			return result, errors.New("missing encoded packet hash or track")
		}
		result.Tracks[packet.Type] = append(result.Tracks[packet.Type], packet.Hash)
	}
	if len(result.Config) != 2 || len(result.Tracks["video"]) == 0 || len(result.Tracks["audio"]) == 0 {
		return result, errors.New("encoded media requires both tracks")
	}
	return result, nil
}

func verifyLiveIdentity(capture, source recordingIdentity, encoded, sourceEncoded encodedMediaIdentity) (recordingIdentityResult, error) {
	if len(source.Tracks["video"]) == 0 || len(capture.Tracks["video"]) == 0 || len(source.Tracks["audio"]) != len(source.Audio) {
		return recordingIdentityResult{}, errors.New("missing live capture identity")
	}
	preroll := 0
	for preroll < len(source.Audio) && source.Tracks["audio"][preroll].DTS < source.Tracks["video"][0].DTS {
		preroll++
	}
	if len(sourceEncoded.Tracks["video"]) != len(source.Video) || len(sourceEncoded.Tracks["audio"]) != len(source.Audio) || len(encoded.Tracks["video"]) != len(capture.Video) || len(encoded.Tracks["audio"]) != len(capture.Audio) {
		return recordingIdentityResult{}, errors.New("encoded and decoded packet counts differ")
	}
	if !reflect.DeepEqual(encoded.Config, sourceEncoded.Config) || len(encoded.Config) != 2 || !reflect.DeepEqual(encoded.Tracks["video"], sourceEncoded.Tracks["video"]) || !reflect.DeepEqual(encoded.Tracks["audio"], sourceEncoded.Tracks["audio"][preroll:]) {
		return recordingIdentityResult{}, errors.New("live encoded payload or codec configuration changed")
	}
	if capture.Tracks["video"][0].DTS != source.Tracks["video"][0].DTS {
		return recordingIdentityResult{}, errors.New("live capture source origin changed")
	}
	reference := source
	reference.Audio = source.Audio[preroll:]
	reference.Tracks = map[string][]recordingPacket{"video": source.Tracks["video"], "audio": source.Tracks["audio"][preroll:]}
	results, err := verifyContinuousMedia([]recordingIdentity{capture}, reference, 60000000, 1, false)
	if err != nil {
		return recordingIdentityResult{}, err
	}
	return results[0], nil
}

func (harness *harness) startLiveIdentityCaptures() (map[string]*process, error) {
	captures := map[string]*process{}
	for _, app := range []string{"flv", "ts"} {
		if err := harness.until("publisher for complete "+app+" capture", 5*time.Second, func() (bool, error) {
			report, err := harness.relayStat(app, "pub")
			return err == nil && report.Count == 1 && len(report.List) == 1 && report.List[0].VideoReceived > 0, err
		}); err != nil {
			return nil, err
		}
		for protocol, address := range map[string]string{"rtmp": fmt.Sprintf("rtmp://127.0.0.1:%d/%s/sample", harness.rtmpPort, app), "http-flv": harness.origin + "/hfs/" + app + "/sample.flv"} {
			name := app + "-" + protocol
			arguments := []string{"ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-y", "-copyts", "-rw_timeout", "8000000"}
			if protocol == "http-flv" {
				arguments = append(arguments, "-multiple_requests", "1")
			}
			arguments = append(arguments, "-i", address, "-map", "0:v:0", "-map", "0:a:0", "-c", "copy", "-f", "flv", filepath.Join(harness.evidence, "capture-"+name+".flv"))
			child, err := harness.launch("capture-"+name, arguments...)
			if err != nil {
				return nil, err
			}
			captures[name] = child
		}
	}
	return captures, nil
}

func (harness *harness) finishLiveIdentityCaptures(captures map[string]*process, source recordingIdentity) error {
	sourceEncoded, err := inspectEncodedMedia(harness.ctx, source.File)
	if err != nil {
		return err
	}
	results := map[string]recordingIdentityResult{}
	for name, child := range captures {
		if err := harness.finish("capture-"+name, child, 10*time.Second); err != nil {
			return err
		}
		filename := filepath.Join(harness.evidence, "capture-"+name+".flv")
		if _, err := mediaCommand(harness.ctx, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-i", filename, "-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"); err != nil {
			return err
		}
		capture, err := inspectRecording(harness.ctx, filename)
		if err != nil {
			return err
		}
		encoded, err := inspectEncodedMedia(harness.ctx, filename)
		if err != nil {
			return err
		}
		verified, err := verifyLiveIdentity(capture, source, encoded, sourceEncoded)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		results[name] = verified
	}
	harness.results["live_capture_identity"] = results
	return harness.checkLogs("capture-*.log")
}
