package config

import (
	"net/http"
	"strconv"
	"testing"
)

func TestRotatedSignatureAllowsDelayedIssuerWithoutExtendingKeyLifetime(test *testing.T) {
	rotated := NewSignatureSet("old", 120).next("new", 30, 1000)
	for _, scenario := range []struct {
		now     int64
		expires int64
		status  int
	}{
		{1005, 1125, http.StatusOK},
		{1005, 1126, http.StatusForbidden},
		{1119, 1125, http.StatusOK},
		{1120, 1125, http.StatusOK},
		{1121, 1125, http.StatusGone},
	} {
		token := AntiLeechSign("live/sample", "old", scenario.expires)
		if status := rotated.checkAt("live/sample", token, strconv.FormatInt(scenario.expires, 10), scenario.now); status != scenario.status {
			test.Errorf("delayed issuer now=%d expiration=%d status=%d expected=%d", scenario.now, scenario.expires, status, scenario.status)
		}
	}
	reloaded := rotated.next("new", 30, 1010)
	if len(reloaded.previous) != 1 || reloaded.previous[0].until != 1120 {
		test.Fatal("unrelated reload extended retired key lifetime")
	}
	token := AntiLeechSign("live/sample", "old", 1125)
	if status := reloaded.checkAt("live/sample", token, "1125", 1121); status != http.StatusGone {
		test.Fatalf("late expiry extended key lifetime: %d", status)
	}
}
