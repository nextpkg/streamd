package core

import (
	"fmt"
	"testing"
)

func TestSlabConfiguredBlockSizesAllocateSafely(test *testing.T) {
	for _, maximum := range []int{64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024} {
		test.Run(fmt.Sprint(maximum), func(test *testing.T) {
			slab := NewSlab()
			slab.Init(maximum/4, maximum)
			first := slab.Get(maximum / 8)
			first[0] = 0xa5
			for repeat := 0; repeat < 20; repeat++ {
				block := slab.Get(maximum / 8)
				block[0] = byte(repeat)
			}
			if first[0] != 0xa5 || slab.mark.max != maximum || len(slab.buf) != maximum {
				test.Fatal("configured slab lost the original payload or ignored its block size")
			}
		})
	}
}
