package gop

import (
	"testing"

	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
)

type optimizationCollector struct{ packets []*packet.Packet }

func (collector *optimizationCollector) Write(frame *packet.Packet) error {
	collector.packets = append(collector.packets, frame)
	return nil
}

func TestGOPStartupOrderRetainsSharedPayloads(test *testing.T) {
	cache := NewCache(1)
	frames := []*packet.Packet{
		{Type: packet.PktMetadata, Data: []byte{1}},
		{Type: packet.PktVideo, Data: []byte{0x17, 0, 0, 0, 0, 1}},
		{Type: packet.PktAudio, Data: []byte{0xaf, 0, 0x12, 0x10}},
		{Type: packet.PktVideo, Data: []byte{0x17, 1, 0, 0, 0, 2}},
		{Type: packet.PktAudio, Data: []byte{0xaf, 1, 3}},
		{Type: packet.PktVideo, Data: []byte{0x27, 1, 0, 0, 0, 4}},
	}
	for _, frame := range frames {
		if err := flv.NewDemuxer().Demux(frame); err != nil {
			test.Fatal(err)
		}
		if err := cache.Write(frame); err != nil {
			test.Fatal(err)
		}
	}
	first, second := &optimizationCollector{}, &optimizationCollector{}
	if err := cache.SendTo(first); err != nil {
		test.Fatal(err)
	}
	if err := cache.SendTo(second); err != nil {
		test.Fatal(err)
	}
	if len(first.packets) != len(frames) || len(second.packets) != len(frames) {
		test.Fatal("GOP startup was incomplete")
	}
	for index, original := range frames {
		if first.packets[index] != original || second.packets[index] != original || &first.packets[index].Data[0] != &original.Data[0] {
			test.Fatalf("startup order or shared payload changed at %d", index)
		}
	}
}

func TestGOPRingReusesStorageAndRetiresOldGroups(test *testing.T) {
	cache := newGopCache(2)
	first, second, third := &packet.Packet{TimeStamp: 1}, &packet.Packet{TimeStamp: 2}, &packet.Packet{TimeStamp: 3}
	if err := cache.Write(first, true); err != nil {
		test.Fatal(err)
	}
	storage := cache.gops[0]
	if err := cache.Write(second, true); err != nil {
		test.Fatal(err)
	}
	if err := cache.Write(third, true); err != nil {
		test.Fatal(err)
	}
	collector := &optimizationCollector{}
	if err := cache.SendTo(collector); err != nil {
		test.Fatal(err)
	}
	if cache.gops[0] != storage || len(collector.packets) != 2 || collector.packets[0] != second || collector.packets[1] != third {
		test.Fatal("GOP ring allocated replacement storage or retained the wrong groups")
	}
}
