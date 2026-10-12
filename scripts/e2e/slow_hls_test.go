package main

import "testing"

func TestSlowHLSErrorAllowlistIsBoundToOwnedConnections(test *testing.T) {
	valid := `{"level":"ERROR","msg":"write tcp 127.0.0.1:1234->127.0.0.1:4321: i/o timeout"}`
	allowed := map[string]bool{"127.0.0.1:4321": true}
	counts, err := slowHLSErrors(valid, "http://127.0.0.1:1234", allowed)
	if err != nil || counts["127.0.0.1:4321"] != 1 {
		test.Fatalf("owned failure missing: %v %v", counts, err)
	}
	for _, body := range []string{valid + "\n" + valid, `{"level":"ERROR","msg":"write tcp 127.0.0.1:1234->127.0.0.1:9999: i/o timeout"}`, `{"level":"ERROR","msg":"write tcp 127.0.0.1:1234->127.0.0.1:4321: broken pipe"}`, `{"level":"ERROR","msg":"media corruption"}`, "WARNING: DATA RACE"} {
		if _, err := slowHLSErrors(body, "http://127.0.0.1:1234", allowed); err == nil {
			test.Fatal("unexpected failure hidden")
		}
	}
}
