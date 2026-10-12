package main

import (
	"testing"
	"time"
)

func adminStageFixture() map[string]any {
	snapshot := map[string]any{"keys": map[string]any{"count": float64(2), "list": []any{"ts/sample", "flv/sample"}}}
	for _, app := range []string{"flv", "ts"} {
		for label, count := range map[string]int{"pub": 1, "sub": 3} {
			rows := []any{}
			for range count {
				rows = append(rows, map[string]any{"key": app + "/sample", "pts": float64(100), "duration": float64(2), "videoReceived": float64(1000), "audioReceived": float64(100), "videoSpeed": float64(500), "audioSpeed": float64(50)})
			}
			snapshot[app+"-"+label] = map[string]any{"count": float64(count), "list": rows}
		}
		snapshot[app+"-online"] = map[string]any{"count": float64(2), "checkin": float64(time.Now().Unix())}
	}
	return snapshot
}

func TestAdminStatisticsEvidenceSeparatesInternalAndExternalConsumers(test *testing.T) {
	if err := validateAdminStage(adminStageFixture(), 1, 3, 2, true); err != nil {
		test.Fatal(err)
	}
	mutations := []func(map[string]any){
		func(snapshot map[string]any) { snapshot["keys"].(map[string]any)["count"] = float64(1) },
		func(snapshot map[string]any) {
			snapshot["keys"].(map[string]any)["list"] = []any{"flv/sample", "flv/sample"}
		},
		func(snapshot map[string]any) { snapshot["flv-online"].(map[string]any)["count"] = float64(3) },
		func(snapshot map[string]any) { snapshot["flv-online"].(map[string]any)["checkin"] = float64(0) },
		func(snapshot map[string]any) { snapshot["flv-pub"].(map[string]any)["list"] = nil },
		func(snapshot map[string]any) {
			snapshot["ts-sub"].(map[string]any)["list"].([]any)[0].(map[string]any)["key"] = "other/sample"
		},
		func(snapshot map[string]any) {
			snapshot["ts-sub"].(map[string]any)["list"].([]any)[0].(map[string]any)["videoReceived"] = float64(0)
		},
		func(snapshot map[string]any) {
			snapshot["ts-sub"].(map[string]any)["list"].([]any)[0].(map[string]any)["audioSpeed"] = float64(-1)
		},
		func(snapshot map[string]any) {
			snapshot["ts-sub"].(map[string]any)["list"].([]any)[0].(map[string]any)["unknown"] = true
		},
	}
	for index, mutate := range mutations {
		snapshot := adminStageFixture()
		mutate(snapshot)
		if err := validateAdminStage(snapshot, 1, 3, 2, true); err == nil {
			test.Fatalf("accepted mutation %d", index)
		}
	}
}
