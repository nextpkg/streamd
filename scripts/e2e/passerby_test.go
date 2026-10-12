package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPasserbyEventsRequireExactRetirementsAndRejectUnrelatedErrors(test *testing.T) {
	event := func(level, message string) string {
		body, err := json.Marshal(map[string]string{"level": level, "msg": message})
		if err != nil {
			test.Fatal(err)
		}
		return string(body) + "\n"
	}
	retirements := []string{}
	for _, key := range []string{"idle/sample", "keep/fresh", "stateful/sample", "hls/sample"} {
		retirements = append(retirements, event("INFO", "["+key+"]Data source idling over 3s."))
	}
	valid := strings.Join(retirements, "")
	counts, warnings, err := validatePasserbyEvents(valid + event("WARN", "Not continuous timestamp"))
	if err != nil || len(counts) != 4 || warnings != 1 {
		test.Fatalf("valid events rejected or warning hidden: counts=%v warnings=%d error=%v", counts, warnings, err)
	}
	for name, body := range map[string]string{
		"missing-retirement":       strings.Join(retirements[1:], ""),
		"duplicate-retirement":     valid + retirements[0],
		"wrong-stream-retired":     valid + event("INFO", "[keep/sample]Data source idling over 3s."),
		"storage-error":            valid + event("ERROR", "recording failed"),
		"warning-with-error-level": valid + event("ERROR", "Not continuous timestamp"),
		"unknown-warning":          valid + event("WARN", "packet lost"),
		"wrong-idle-deadline":      strings.Replace(valid, "over 3s.", "over 30s.", 1),
		"panic-text":               valid + "panic: nil pointer\n",
	} {
		test.Run(name, func(test *testing.T) {
			if _, _, err := validatePasserbyEvents(body); err == nil {
				test.Fatal("invalid retirement evidence accepted")
			}
		})
	}
}
