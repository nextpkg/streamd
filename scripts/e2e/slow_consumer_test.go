package main

import "testing"

func TestGoroutineMetricRejectsMissingDuplicateAndInvalidValues(test *testing.T) {
	for _, body := range []string{"", "go_goroutines NaN", "go_goroutines 0", "go_goroutines -1", "go_goroutines 3\ngo_goroutines 3", "go_goroutines{label=\"x\"} 5"} {
		if _, err := parseGoroutineMetric(body); err == nil {
			test.Fatalf("accepted %q", body)
		}
	}
	if count, err := parseGoroutineMetric("# TYPE go_goroutines gauge\ngo_goroutines 42\n"); err != nil || count != 42 {
		test.Fatalf("valid gauge: %d %v", count, err)
	}
}

func TestSlowConsumerErrorEvidenceRequiresExactEvictions(test *testing.T) {
	valid := ""
	for _, app := range []string{"flv", "flv", "ts", "ts"} {
		valid += `{"level":"ERROR","msg":"rtmp IO timeout. elapse=11s. limit=10s"}` + "\n"
		valid += `{"level":"ERROR","msg":"[` + app + `/sample]Writer is timeout"}` + "\n"
	}
	if _, err := validateSlowConsumerLog(valid); err != nil {
		test.Fatal(err)
	}
	for _, body := range []string{valid + `{"level":"ERROR","msg":"storage broken"}`, valid + valid, "", `{"level":"ERROR","msg":"rtmp IO timeout. elapse=11s. limit=0s"}`} {
		if _, err := validateSlowConsumerLog(body); err == nil {
			test.Fatal("accepted missing, duplicated or unexpected failure")
		}
	}
}
