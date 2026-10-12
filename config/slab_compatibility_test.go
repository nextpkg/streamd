package config

import "testing"

func TestSlabCLIOverridesPrecedence(test *testing.T) {
	previous := slabOverride.Load()
	test.Cleanup(func() { slabOverride.Store(previous) })
	if err := SetSlabOverrides(64, 512); err != nil {
		test.Fatal(err)
	}
	for repeat := 0; repeat < 2; repeat++ {
		settings := NewConfig().RTMP
		settings.SlabMin, settings.SlabMax = 16, 128
		if err := settings.validate(); err != nil {
			test.Fatal(err)
		}
		if settings.SlabMin != 64 || settings.SlabMax != 512 {
			test.Fatal("configuration reload lost CLI override precedence")
		}
	}
	if err := SetSlabOverrides(0, 0); err != nil {
		test.Fatal(err)
	}
	settings := NewConfig().RTMP
	settings.SlabMin, settings.SlabMax = 16, 128
	if err := settings.validate(); err != nil || settings.SlabMin != 16 || settings.SlabMax != 128 {
		test.Fatalf("zero overrides changed configuration: %+v %v", settings, err)
	}
}

func TestSlabThresholdValidationPreservesLastOverride(test *testing.T) {
	previous := slabOverride.Load()
	test.Cleanup(func() { slabOverride.Store(previous) })
	if err := SetSlabOverrides(32, 256); err != nil {
		test.Fatal(err)
	}
	for _, sizes := range [][2]int{{-1, 256}, {32, -1}, {512, 256}} {
		if err := SetSlabOverrides(sizes[0], sizes[1]); err == nil {
			test.Fatal("invalid CLI override accepted")
		}
		if current := slabOverride.Load(); current.minimum != 32 || current.maximum != 256 {
			test.Fatal("invalid CLI override replaced confirmed values")
		}
	}
	if err := SetSlabOverrides(0, 0); err != nil {
		test.Fatal(err)
	}
	for _, sizes := range [][2]int{{-1, 256}, {32, -1}, {512, 256}, {512, 0}} {
		settings := NewConfig().RTMP
		settings.SlabMin, settings.SlabMax = sizes[0], sizes[1]
		if err := settings.validate(); err == nil {
			test.Fatalf("invalid configured thresholds accepted: %v", sizes)
		}
	}
}
