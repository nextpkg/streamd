package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"path"
	"strings"
)

func validateCompletionCallbacks(observations []callbackObservation, files, cloud []string, started int64) (map[string]int, error) {
	expected := map[string][2]int{}
	if len(cloud) != 2 || len(files) < 2 {
		return nil, errors.New("missing independent cloud/file evidence")
	}
	for _, key := range cloud {
		identity := "initial/" + key
		if expected[identity] != [2]int{} {
			return nil, errors.New("duplicate cloud identity")
		}
		expected[identity] = [2]int{2, 0}
	}
	for _, key := range files {
		identity := "recovered/" + key
		if expected[identity] != [2]int{} {
			return nil, errors.New("duplicate recovered identity")
		}
		expected[identity] = [2]int{1, 1}
	}
	actual := map[string][2]int{}
	counts := map[string]int{}
	starts := map[string]float64{}
	ends := map[string]float64{}
	for _, observation := range observations {
		payload := observation.Payload
		event, eventOK := payload["event"].(string)
		key, keyOK := payload["key"].(string)
		occur, timeOK := payload["occur"].(float64)
		if !eventOK || !keyOK || (key != "flv/sample" && key != "ts/sample") || !timeOK || math.IsNaN(occur) || math.IsInf(occur, 0) || occur < float64(started) || occur > float64(observation.Received) || observation.Status != http.StatusOK {
			return nil, errors.New("invalid completion callback identity, timestamp or status")
		}
		counts[key+"/"+event]++
		switch event {
		case "OnPublish", "OnPublishDone":
			if len(payload) != 3 || observation.Path != "/publication" {
				return nil, errors.New("publication callback target changed")
			}
		case "OnRecord", "OnRecordDone":
			location, locationOK := payload["location"].(string)
			connection, connectionOK := payload["connInfo"].(map[string]any)
			app, _, _ := strings.Cut(key, "/")
			if len(payload) != 6 || !locationOK || !connectionOK || connection["app"] != app || path.Dir(location) != "/"+key || path.Ext(location) != "."+app || payload["filename"] != path.Base(location) {
				return nil, errors.New("recording callback content mismatch")
			}
			identity := strings.TrimPrefix(observation.Path, "/") + location
			if _, exists := expected[identity]; !exists {
				return nil, errors.New("unknown recording callback identity")
			}
			pair := actual[identity]
			if event == "OnRecord" {
				pair[0]++
				starts[identity] = max(starts[identity], occur)
			} else {
				pair[1]++
				ends[identity] = occur
			}
			actual[identity] = pair
		default:
			return nil, errors.New("unexpected completion callback")
		}
	}
	for identity, pair := range expected {
		if actual[identity] != pair || (pair[1] != 0 && ends[identity] < starts[identity]) {
			return nil, fmt.Errorf("missing/duplicate/false completion for %s: got %v expected %v", identity, actual[identity], pair)
		}
	}
	for _, app := range []string{"flv", "ts"} {
		if counts[app+"/sample/OnPublish"] != 2 || counts[app+"/sample/OnPublishDone"] != 2 {
			return nil, errors.New("seed and resumed publisher lifecycle counts differ")
		}
	}
	return counts, nil
}

func completionOutcomeErrors(body []byte, mode string) ([]string, error) {
	var expected []string
	counts := map[string]int{}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		var entry struct{ Level, Msg string }
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, err
		}
		if entry.Level != "ERROR" {
			continue
		}
		prefix := ""
		for _, candidate := range []string{"close failed,sync=true,block=true:", "[flv/sample]recording configuration finalization: close failed,sync=true,block=true:", "[ts/sample]recording configuration finalization: close failed,sync=true,block=true:"} {
			if strings.HasPrefix(entry.Msg, candidate) {
				prefix = candidate
			}
		}
		matched := mode == "cleanup-denied" && strings.Contains(entry.Msg, "remove completed recording checkpoint") && strings.Contains(entry.Msg, "AccessDenied")
		if mode == "response-lost" {
			matched = strings.Contains(entry.Msg, "NoSuchUpload") || strings.Contains(entry.Msg, "EOF")
		}
		if prefix == "" || !matched {
			return expected, fmt.Errorf("unexpected completion error: %s", entry.Msg)
		}
		counts[prefix]++
		expected = append(expected, entry.Msg)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(expected) != 4 || counts["close failed,sync=true,block=true:"] != 2 || counts["[flv/sample]recording configuration finalization: close failed,sync=true,block=true:"] != 1 || counts["[ts/sample]recording configuration finalization: close failed,sync=true,block=true:"] != 1 {
		return expected, errors.New("completion failures not accounted for both recorders")
	}
	return expected, nil
}
