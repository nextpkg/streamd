// Package core 处理 RTMP 连接、控制消息及媒体传输。
package core

import (
	"github.com/nextpkg/streamd/base"
	errors "github.com/nextpkg/streamd/internal/errutil"
)

// ConnServer RTMP服务端
type ConnServer struct {
	done          bool   // 标志是否完成指令消息读取的流程
	method        string // 控制指令, "play", "publish"等
	duration      int    // getStreamLength 响应值，当前固定为零
	streamID      uint32
	transactionID uint32
	conn          *Conn
	connect       base.ConnectInfo
	publish       base.PublishInfo
}

// NewConnServer RTMP服务端
func NewConnServer(conn *Conn, chunkSize uint32) *ConnServer {
	// 针对server调整默认的chunkSize
	conn.chunkSize = chunkSize
	if conn.chunkSize < 128 {
		panic("chunk size < 128")
	}

	return &ConnServer{
		streamID: 1,
		conn:     conn,
	}
}

// GetInfo 返回Info的字段值
func (s *ConnServer) GetInfo() (string, string) {
	return s.connect.App, s.publish.Name
}

// GetPublish 获取发布流的信息
func (s *ConnServer) GetPublish() *base.PublishInfo {
	return &s.publish
}

// GetConnect 获取RTMP连接的信息
func (s *ConnServer) GetConnect() *base.ConnectInfo {
	return &s.connect
}

// Method RTMP控制指令
func (s *ConnServer) Method() string {
	return s.method
}

// ===================================

// Write 向客户端写数据
func (s *ConnServer) Write(cs *ChunkStream) error {
	// 接收到chunk后，对metadata进行处理
	err := cs.unpack()
	if err != nil {
		return errors.Wrap(err, "handle metadata failed")
	}

	return s.conn.Write(cs)
}

// Read 从客户端读数据
func (s *ConnServer) Read(cs *ChunkStream) error {
	for {
		err := s.conn.Read(cs)
		if err != nil {
			return err
		}

		switch cs.typeID {
		case 15, 18:
			err = s.handleDataMsg(cs)
			if err != nil {
				return errors.Wrap(err, "handle data message failed")
			}
		case 20:
			err = s.handleCommandMsg(cs)
			if err != nil {
				return errors.Wrap(err, "handle command message failed")
			}

			continue
		}

		return nil
	}
}

// Close 关闭服务端
func (s *ConnServer) Close() error {
	return s.conn.Close()
}

// Flush 强制清空缓冲
func (s *ConnServer) Flush() error {
	return s.conn.Flush()
}

// ===================================

// CommandLinkup 控制指令的读与响应。读取消息直到客户端请求开始播放或者发布，一个实例只需要被执行一次
func (s *ConnServer) CommandLinkup() error {

	for {
		cs := &ChunkStream{}

		err := s.conn.Read(cs)
		if err != nil {
			return err
		}

		err = s.handleCommandMsg(cs)
		if err != nil {
			return errors.Wrap(err, "handle command message failed")
		}

		// 已经publish或者play则退出循环
		if s.done {
			break
		}
	}

	return nil
}
