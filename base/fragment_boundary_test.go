package base

import (
	"math"
	"testing"
)

func TestFragmentCandidateBoundaryDoesNotConsumePacket(test *testing.T) {
	initial := NewFragment(2)
	if initial.DurationAt(500) != 2000 || initial.started {
		test.Fatal("initial segment setup changed")
	}
	for _, start := range []uint32{0, 1000, math.MaxUint32 - 1000} {
		fragment := NewFragment(0)
		fragment.Update(start)
		fragment.Update(start + 1960)
		before := *fragment
		for _, sample := range []struct {
			timestamp uint32
			want      int64
		}{{start, 0}, {start + 1999, 1999}, {start + 2000, 2000}, {start - 1, 0}} {
			if got := fragment.DurationAt(sample.timestamp); got != sample.want {
				test.Errorf("start=%d candidate=%d elapsed=%d want=%d", start, sample.timestamp, got, sample.want)
			}
			if *fragment != before {
				test.Fatal("candidate boundary mutated fragment state")
			}
		}
	}
}
