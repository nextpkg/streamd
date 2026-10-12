package base

import (
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func fragmentClockSubprocess(test *testing.T, expectedWarnings int) {
	test.Helper()
	command := exec.Command(os.Args[0], "-test.run=^"+test.Name()+"$")
	command.Env = append(os.Environ(), "STREAMD_FRAGMENT_CLOCK_CHILD=1")
	body, err := command.CombinedOutput()
	if err != nil {
		test.Fatalf("fragment subprocess: %v: %s", err, body)
	}
	if actual := strings.Count(string(body), `"msg":"Not continuous timestamp"`); actual != expectedWarnings {
		test.Fatalf("clock warnings = %d, want %d: %s", actual, expectedWarnings, body)
	}
}

func TestFragmentCoincidentTrackClocks(test *testing.T) {
	if os.Getenv("STREAMD_FRAGMENT_CLOCK_CHILD") != "1" {
		fragmentClockSubprocess(test, 0)
		return
	}
	for _, start := range []uint32{0, 6060, math.MaxUint32 - 10} {
		fragment := NewFragment(2)
		fragment.Update(start)
		fragment.Update(start)
		if fragment.Duration() != 0 {
			test.Fatal("coincident audio/video timestamps advanced the clock")
		}
		fragment.Update(start + 20)
		fragment.Update(start + 20)
		if fragment.Duration() != 20 {
			test.Fatal("clock failed to advance after coincident timestamps")
		}
		fragment.Create()
		fragment.Update(start)
		fragment.Update(start)
		if fragment.Duration() != 0 || fragment.Sequence() != "1" {
			test.Fatal("new fragment retained the preceding clock")
		}
	}
}

func TestFragmentRegressedOriginStillWarns(test *testing.T) {
	if os.Getenv("STREAMD_FRAGMENT_CLOCK_CHILD") != "1" {
		fragmentClockSubprocess(test, 6)
		return
	}
	for _, start := range []uint32{0, 6060, math.MaxUint32 - 10} {
		fragment := NewFragment(2)
		fragment.Update(start)
		fragment.Update(start + 20)
		fragment.Update(start)
		fragment.Update(start - 1)
		if fragment.Duration() != 20 {
			test.Fatal("regression replaced the accepted clock")
		}
	}
}
