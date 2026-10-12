package mediatime

import (
	"math"
	"testing"
)

func TestTailUsesPresentationAndCadence(test *testing.T) {
	var tail Tail
	tail.ObserveReverse(1, 90, 90)
	tail.ObserveReverse(0, 80, 120)
	tail.ObserveReverse(1, 67, 67)
	tail.ObserveReverse(0, 40, 160)
	tail.ObserveReverse(0, 0, 80)
	offset, err := tail.Next()
	if err != nil || offset != 200 {
		test.Fatalf("offset=%d error=%v", offset, err)
	}
}

func TestTailRejectsMissingMediaAndOverflow(test *testing.T) {
	var tail Tail
	if _, err := tail.Next(); err == nil {
		test.Fatal("missing timestamps accepted")
	}
	tail.ObserveReverse(0, math.MaxUint32, math.MaxUint32)
	if _, err := tail.Next(); err == nil {
		test.Fatal("timestamp overflow accepted")
	}
}
