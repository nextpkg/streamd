// Package flv 解析和封装 FLV 标签及媒体数据。
package flv

import (
	"github.com/nextpkg/streamd/packet"
)

// Demuxer FLV 解复用器
type Demuxer struct{}

// NewDemuxer FLV 解复用器
func NewDemuxer() *Demuxer {
	return &Demuxer{}
}

// Demux flv解复用
// 解析 p.Data 中的 FLV 媒体头，并设置 p.Header 和 p.Media。
func (d *Demuxer) Demux(p *packet.Packet) error {

	var tag Tag

	// 解析媒体头，n 为媒体头所占字节数
	n, err := tag.ParseMediaTagHeader(p.Data, p.Type)
	if err != nil {
		return err
	}

	p.Header = &tag
	p.Media = p.Data[n:]

	return nil
}
