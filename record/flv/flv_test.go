package flv

import (
	"os"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/record/storage"
	"github.com/stretchr/testify/assert"
)

func TestNewFlvRecord(t *testing.T) {
	at := assert.New(t)
	previous := config.Get().Metadata
	config.Get().Metadata = config.Metadata{Provider: "test-provider1", Service: "Streaming media process service"}
	t.Cleanup(func() { config.Get().Metadata = previous })
	directory := t.TempDir()

	d := flv.NewDemuxer()

	info := base.NewInfo("t_app", "t_ins", false)

	config.Get().SetApp("t_app", &config.App{
		Enable:  true,
		AppName: "t_app",
		Recorder: config.AppRecorder{
			Enable:   true,
			SaveTo:   directory,
			SaveAs:   "flv",
			SaveMode: "round",
			Driver:   "fs",
			Segment:  900,
		},
	})

	at.Nil(os.RemoveAll(directory))

	param := config.Get().GetRecorder("t_app")
	at.NotNil(param)

	store := storage.NewStorage(info, param, storage.NewEvent())
	at.NotNil(store)

	s := NewStream(info, store)

	// 视频序列头
	p1 := &packet.Packet{
		Type: packet.PktVideo,
		Data: []byte{
			0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64, 0x00,
			0x1f, 0xff, 0xe1, 0x00, 0x19, 0x67, 0x64, 0x00,
			0x1f, 0xac, 0x24, 0x84, 0x01, 0x40, 0x16, 0xec,
			0x04, 0x40, 0x00, 0x00, 0x03, 0x00, 0x40, 0x00,
			0x00, 0x0c, 0x23, 0xc6, 0x0c, 0x92, 0x01, 0x00,
			0x05, 0x68, 0xee, 0x32, 0xc8, 0xb0,
		},
	}
	at.Nil(d.Demux(p1))
	at.Nil(s.Write(p1))

	// 视频I帧
	p2 := &packet.Packet{
		Type: packet.PktVideo,
		Data: []byte{
			0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		},
	}
	at.Nil(d.Demux(p2))
	at.Nil(s.Write(p2))

	// 音频序列头
	p3 := &packet.Packet{
		Type: packet.PktAudio,
		Data: []byte{
			0xaf, 0x00, 0x12, 0x10,
		},
	}
	at.Nil(d.Demux(p3))
	at.Nil(s.Write(p3))

	// 音频数据
	p4 := &packet.Packet{
		Type: packet.PktAudio,
		Data: []byte{
			0xaf, 0x01, 0x21, 0x00, 0x49, 0x90, 0x02, 0x19, 0x00, 0x23, 0x80,
		},
	}
	at.Nil(d.Demux(p4))
	at.Nil(s.Write(p4))

	s.Close()
	s.Wait()

	data, err := store.ReadLastFile()
	at.Nil(err)
	at.Equal(244, len(data))

	at.Nil(os.RemoveAll(directory))
}
