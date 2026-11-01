package rtmp

import (
	"bytes"

	"github.com/nextpkg/goav/amf"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/errutil"
	"github.com/nextpkg/streamd/packet"
)

// writeCache 缓存音视频数据包
func (s *Stream) writeCache(p *packet.Packet) error {
	c := s.inside.cache

	switch p.Type {
	case packet.PktVideo:
		err := c.WriteVideo(p)
		if err != nil {
			return errutil.Wrap(err, "write video failed")
		}
	case packet.PktAudio:
		err := c.WriteAudio(p)
		if err != nil {
			return errutil.Wrap(err, "write audio failed")
		}
	case packet.PktMetadata:
		publish := s.rConn.GetPublish()
		if publish.MetaData == nil {
			publish.MetaData = make(map[string]interface{})
		}

		// 补充Metadata
		cfg := config.Get().Metadata
		publish.MetaData["Provider"] = cfg.Provider
		publish.MetaData["Service"] = cfg.Service

		// 重新封装为metadata，格式: [OnMetaData, data]
		cmdBuf := bytes.NewBuffer(nil)
		err := amf.NewEnDecAMF0().EncodeBatch(cmdBuf, amf.OnMetaData, amf.Object(publish.MetaData))
		if err != nil {
			return errutil.Wrap(err, "amf0 batch encode metadata failed")
		}

		// 缓存metadata包
		p.Data = cmdBuf.Bytes()
		c.Metadata.Write(p)
	}

	return nil
}
