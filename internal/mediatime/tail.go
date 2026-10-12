package mediatime

import (
	"errors"
	"math"
)

type Tail struct {
	latest   map[int]int64
	interval map[int]int64
	maximum  int64
	count    int
}

func (tail *Tail) ObserveReverse(kind int, decoding, presentation int64) {
	if tail.latest == nil {
		tail.latest, tail.interval = make(map[int]int64), make(map[int]int64)
	}
	tail.maximum = max(tail.maximum, decoding, presentation)
	if latest, exists := tail.latest[kind]; !exists {
		tail.latest[kind] = decoding
	} else if tail.interval[kind] == 0 && latest > decoding {
		tail.interval[kind] = latest - decoding
	}
	tail.count++
}

func (tail *Tail) Next() (uint32, error) {
	if tail.count == 0 {
		return 0, errors.New("recording contains no recoverable media timestamps")
	}
	interval := int64(1)
	for _, cadence := range tail.interval {
		interval = max(interval, cadence)
	}
	next := tail.maximum + interval
	if next > math.MaxUint32 {
		return 0, errors.New("recording timestamp exceeds supported range")
	}
	return uint32(next), nil
}
