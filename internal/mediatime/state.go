package mediatime

import (
	"errors"
	"math"
)

type State struct {
	Format string `json:"format"`
	Bytes  int64  `json:"bytes"`
	Next   uint64 `json:"next_ms"`
}

func (state *State) Clone() *State {
	if state == nil {
		return nil
	}
	copy := *state
	return &copy
}

func (state *State) Validate(size int64) error {
	if state == nil || (state.Format != "flv" && state.Format != "ts") || state.Bytes != size || size <= 0 || state.Next == 0 || state.Next > math.MaxUint32 {
		return errors.New("invalid media checkpoint format, extent or timestamp")
	}
	return nil
}

func (tail *Tail) ObserveForward(kind int, decoding, presentation int64) {
	if tail.latest == nil {
		tail.latest, tail.interval = make(map[int]int64), make(map[int]int64)
	}
	if previous, exists := tail.latest[kind]; exists && decoding > previous {
		tail.interval[kind] = decoding - previous
	}
	tail.latest[kind] = decoding
	tail.maximum = max(tail.maximum, decoding, presentation)
	tail.count++
}

func (tail *Tail) NextValue() uint64 {
	if tail.count == 0 {
		return 0
	}
	interval := int64(1)
	for _, cadence := range tail.interval {
		interval = max(interval, cadence)
	}
	return uint64(tail.maximum + interval)
}
