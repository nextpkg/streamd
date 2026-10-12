package main

import (
	"maps"
	"testing"
)

func TestInspectionRetryRequiresSavedEvidenceAndTailOverwrite(test *testing.T) {
	prepared := map[string]inspectionRetryEvidence{
		"flv/sample/1.flv": {UploadID: "flv-upload", CheckpointSHA256: "saved-flv", FullPartETag: "prefix-flv", TailBytes: 17, RejectedCompletions: 1},
		"ts/sample/1.ts":   {UploadID: "ts-upload", CheckpointSHA256: "saved-ts", FullPartETag: "prefix-ts", TailBytes: 23, RejectedCompletions: 1},
	}
	writes := map[string][]int{"flv/sample/1.flv": {2}, "ts/sample/1.ts": {2}, "flv/sample/2.flv": {1}}
	if err := validateInspectionRetryWrites(prepared, writes); err != nil {
		test.Fatal(err)
	}
	for _, sequence := range [][]int{nil, {1}, {3}, {2, 2}, {2, 3}} {
		corrupted := maps.Clone(writes)
		corrupted["ts/sample/1.ts"] = sequence
		if err := validateInspectionRetryWrites(prepared, corrupted); err == nil {
			test.Fatalf("incorrect recovery accepted: %v", sequence)
		}
	}
	for _, alter := range []func(map[string]inspectionRetryEvidence){
		func(values map[string]inspectionRetryEvidence) { delete(values, "ts/sample/1.ts") },
		func(values map[string]inspectionRetryEvidence) {
			value := values["ts/sample/1.ts"]
			value.CheckpointSHA256 = ""
			values["ts/sample/1.ts"] = value
		},
		func(values map[string]inspectionRetryEvidence) {
			value := values["ts/sample/1.ts"]
			value.RejectedCompletions = 0
			values["ts/sample/1.ts"] = value
		},
	} {
		corrupted := maps.Clone(prepared)
		alter(corrupted)
		if err := validateInspectionRetryWrites(corrupted, writes); err == nil {
			test.Fatal("incomplete failed-completion seed accepted")
		}
	}
}
