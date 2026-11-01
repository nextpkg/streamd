// Package core 服务端与客户端的操作界面
package core

import (
	"fmt"
	"sync"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/container/flv"
	errors "github.com/nextpkg/streamd/internal/errutil"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/metrics"
	"github.com/nextpkg/streamd/packet"
	"sync/atomic"
)

// Reader 用以从外部连接中读取数据
type Reader struct {
	*base.Active
	*base.Stat
	info       *base.Info
	flv        *flv.Demuxer    // RTMP解复用
	conn       readWriteCloser // 读写工具，注意，读写应该分别拥有独立缓冲区
	prefetched *packet.Packet
	closeOnce  sync.Once
}

// NewReader 用以从外部连接中读取数据, IsExternal: false
func NewReader(conn readWriteCloser) *Reader {
	app, instance := conn.GetInfo()

	return &Reader{
		Active: base.NewRwAlive(),
		Stat:   base.NewStat(),
		info:   base.NewInfo(app, instance, false),
		flv:    flv.NewDemuxer(),
		conn:   conn,
	}
}

// Info Reader描述信息
func (r *Reader) Info() *base.Info {
	return r.info
}

// Read 从conn读取数据
func (r *Reader) Read(p *packet.Packet) error {
	if r.prefetched != nil {
		*p = *r.prefetched
		r.prefetched = nil
		return nil
	}
	var cs *ChunkStream

ReadChunk:
	for {
		cs = &ChunkStream{}

		// 从"conn"中读取一个数据"chunk"
		err := r.conn.Read(cs)
		if err != nil {
			return err
		}

		// 更新读时间
		r.Keepalive()

		// 类型转换
		switch cs.typeID {
		case packet.TagVideo:
			p.Type = packet.PktVideo
			break ReadChunk
		case packet.TagAudio:
			p.Type = packet.PktAudio
			break ReadChunk
		case packet.TagScriptDataAMF0, packet.TagScriptDataAMF3:
			p.Type = packet.PktMetadata
			break ReadChunk
		}
	}

	p.TimeStamp = cs.timestamp
	p.Baseline = cs.timestamp + r.GetBaseTime()
	p.StreamID = cs.streamID
	p.Data = cs.data

	err := r.flv.Demux(p)
	if err != nil {
		return errors.Wrap(err, "flv demux failed")
	}

	switch p.Type {
	case packet.PktVideo:
		vh := p.Header.(packet.VideoPacketHeader)
		if !vh.IsCodecAvc() {
			return fmt.Errorf("incompatible video codec(%d)", vh.CodecID())
		}
	case packet.PktAudio:
		ah := p.Header.(packet.AudioPacketHeader)
		if !ah.IsSoundAAC() {
			return fmt.Errorf("incompatible audio codec(%d)", ah.SoundFormat())
		}
	}

	// 更新统计信息
	r.SetMediaTime(p)
	r.Update(p)

	metrics.Average(r.Info().App, r.Info().Instance, metrics.ChannelsBW,
		float64(r.VideoBps()+r.AudioBps())*8,
	)

	return nil
}

func (reader *Reader) Prefetch() error {
	if reader.prefetched != nil {
		return nil
	}
	first := &packet.Packet{}
	if err := reader.Read(first); err != nil {
		return err
	}
	reader.prefetched = first
	return nil
}

// Close 关闭连接
func (reader *Reader) Close() {
	reader.closeOnce.Do(func() {
		if err := reader.conn.Close(); err != nil {
			log.Trace("close failed")
		}
	})
}

// GetPublish 获取底层Publish信息
func (r *Reader) GetPublish() *base.PublishInfo {
	return r.conn.GetPublish()
}

// GetConnect 获取底层Connect信息
func (r *Reader) GetConnect() *base.ConnectInfo {
	return r.conn.GetConnect()
}

// Writer 提供向外输出RTMP的writer封装
type Writer struct {
	*Universal
	status    atomic.Bool
	closeOnce sync.Once
	readDone  chan struct{}

	// 数据源，客户端 or 服务端 连接, 注意，读写应该分别拥有独立缓冲区
	conn readWriteCloser
}

// NewWriter 提供向外输出RTMP的writer封装, IsExternal: true
func NewWriter(conn readWriteCloser) *Funnel {
	app, instance := conn.GetInfo()
	info := base.NewInfo(app, instance, true)

	ret := &Writer{
		Universal: NewUniversal(info),
		conn:      conn,
		readDone:  make(chan struct{}),
	}

	funnel := NewFunnel(ret)
	go func() {
		defer close(ret.readDone)
		ret.connRead()
		funnel.Abort()
	}()
	return funnel
}

// 从客户端读取数据, Read逻辑会有响应客户端的指令
func (w *Writer) connRead() {
	for !w.status.Load() {
		cs := &ChunkStream{}
		err := w.conn.Read(cs)
		if err != nil {
			// rtmp reader was closed
			log.Tracef("[%s]err:%s", w.Info().Key, err)
			break
		}
	}
	w.Close()
}

// Write 写源数据到队列
func (w *Writer) Write(p *packet.Packet) error {
	cs := &ChunkStream{
		data:      p.Data,
		length:    uint32(len(p.Data)),
		streamID:  p.StreamID,
		timestamp: p.Baseline,
	}

	switch p.Type {
	case packet.PktVideo:
		cs.typeID = packet.TagVideo
	case packet.PktAudio:
		cs.typeID = packet.TagAudio
	case packet.PktMetadata:
		cs.typeID = packet.TagScriptDataAMF0
	}

	err := w.conn.Write(cs)
	if err != nil {
		w.Close()
		return errors.Wrapf(err, "[%s]rtmp write failed", w.Info().Key)
	}

	return nil
}

// After 在主流程之后要做的工作
func (w *Writer) After() {
	if !w.status.Load() {
		if playback, ok := w.conn.(interface{ FinishPlayback() error }); ok {
			if err := playback.FinishPlayback(); err != nil {
				log.Trace(err)
			}
		}
	}

	if err := w.conn.Flush(); err != nil {
		log.Trace(err)
	}
	w.Abort()
	<-w.readDone
}

func (w *Writer) Abort() {
	w.Close()
	w.closeOnce.Do(func() {
		if err := w.conn.Close(); err != nil {
			log.Trace(err)
		}
	})
}

// Close 发起关闭流程时要处理的工作
func (w *Writer) Close() {
	w.status.Store(true)
}

// Name writer's name
func (w *Writer) Name() string {
	return "rtmp"
}
