package hls

import (
	"math"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
)

func TestHLSCutsAtCurrentKeyframeRatherThanPreviousPacket(test *testing.T) {
	for _, start := range []uint32{0, 1000, math.MaxUint32 - 1000} {
		info := base.NewInfo(test.Name(), "sample", false)
		config.Get().SetApp(info.App, &config.App{Enable: true, AppName: info.App, HLS: config.AppHLS{Enable: true, Fragment: 2, PlaylistLength: 2}})
		funnel := NewStream(info)
		stream := funnel.Terminal.(*Stream)
		func() {
			defer func() { stream.Close(); funnel.Close(); funnel.Wait() }()
			write := func(kind int, timestamp uint32, body []byte) {
				test.Helper()
				frame := &packet.Packet{Type: kind, Baseline: timestamp, Data: body}
				if err := flv.NewDemuxer().Demux(frame); err != nil {
					test.Fatal(err)
				}
				if err := stream.Write(frame); err != nil {
					test.Fatal(err)
				}
			}
			write(packet.PktVideo, start, []byte{0x17, 0, 0, 0, 0, 1, 0x64, 0, 0x1f, 0xff, 0xe1, 0, 0x19, 0x67, 0x64, 0, 0x1f, 0xac, 0x24, 0x84, 1, 0x40, 0x16, 0xec, 4, 0x40, 0, 0, 3, 0, 0x40, 0, 0, 0x0c, 0x23, 0xc6, 0x0c, 0x92, 1, 0, 5, 0x68, 0xee, 0x32, 0xc8, 0xb0})
			write(packet.PktVideo, start, []byte{0x17, 1, 0, 0, 0, 0, 0, 0, 1, 0x65})
			write(packet.PktVideo, start+1960, []byte{0x27, 1, 0, 0, 0, 0, 0, 0, 1, 0x41})
			if stream.m3u8.GetItem("1") != nil {
				test.Fatal("segment closed before a qualifying keyframe")
			}
			write(packet.PktVideo, start+2000, []byte{0x17, 1, 0, 0, 0, 0, 0, 0, 1, 0x65})
			item := stream.m3u8.GetItem("1")
			if item == nil {
				test.Errorf("start=%d: exact-boundary keyframe skipped because previous packet was early", start)
				return
			}
			if item.Duration() != 2*time.Second {
				test.Errorf("start=%d: segment duration %v, want actual boundary 2s", start, item.Duration())
			}
			if stream.fragment.Duration() != 0 {
				test.Errorf("new segment did not start at the boundary: %d", stream.fragment.Duration())
			}
		}()
	}
}
