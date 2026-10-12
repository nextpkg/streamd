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
