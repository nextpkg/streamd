package core

import "testing"

func BenchmarkSlabAllocation(benchmark *testing.B) {
	slab := NewSlab()
	benchmark.ReportAllocs()
	benchmark.SetBytes(1024)
	for iteration := 0; iteration < benchmark.N; iteration++ {
		block := slab.Get(1024)
		block[0] = byte(iteration)
	}
}

// Keep both implementations' payloads on the heap, as in queued media delivery.
var allocationPayload []byte

func BenchmarkMediaAllocationComparison(b *testing.B) {
	for _, sample := range []struct {
		name string
		size int
	}{{"128B", 128}, {"1KiB", 1024}, {"400KiB", 400 * 1024}} {
		b.Run(sample.name, func(b *testing.B) {
			b.Run("make", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					block := make([]byte, sample.size)
					block[0] = byte(i)
					allocationPayload = block
				}
			})
			b.Run("slab", func(b *testing.B) {
				slab := NewSlab()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					block := slab.Get(sample.size)
					block[0] = byte(i)
					allocationPayload = block
				}
			})
		})
	}
	allocationPayload = nil
}
