// Package core rtmp 收发RTMP分块功能
package core

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nextpkg/streamd/internal/logging"
)

// option RTMP 连接选项
type option struct {
	/**
	消息重组的大小保护阈值，按已读分块数乘以本地 chunkSize 估算。
	*/
	maxChunkSize uint32
	/**
	超时时间，限制网络IO的读写超时
	*/
	timeout time.Duration
}

var defaultOption = option{
	maxChunkSize: 100 * 1024 * 1024,
	timeout:      10 * time.Second,
}

// Conn 收发RTMP分块
type Conn struct {
	net.Conn
	/**
	RTMP 发送 chunk 大小；较小的 chunk 会增加每条消息的分块数量和头部开销。
	*/
	chunkSize       uint32 // 本地的块大小，按此标准写chunk
	remoteChunkSize uint32 // 远程的块大小，按此标准读chunk
	/**
	RTMP 确认窗口以字节计；窗口大小与已确认字节数分别保存。
	*/
	windowAckSize       uint32 // 本地确认窗口（字节）
	remoteWindowAckSize uint32 // 对端要求的 ACK 发送阈值（字节）
	received            uint32 // 已接收的字节数(大于0xf0000000则归零)
	ackReceived         uint32 // 当前 ACK 周期计数；setPeerBandwidth 也会更新此值
	sent                uint32 // 已发送的字节数(大于0xf0000000则归零)
	ackSent             uint32 // 对端 ACK 消息携带的计数
	bandwidthLimitType  byte   // 带宽限制类型
	option              option
	rw                  *ReadWriter             // 网络缓冲
	slab                *Slab                   // chunk内存分配器
	chunks              map[uint32]*ChunkStream // chunk接收
	csID                uint32
	rLocker             sync.Mutex
	wLocker             sync.Mutex
}

// NewConn RTMP连接器
func NewConn(conn net.Conn, bufSize int) *Conn {
	return &Conn{
		Conn:                conn,
		chunkSize:           128,       // 起始值一定是128
		remoteChunkSize:     128,       // 起始值一定是128
		windowAckSize:       2500000,   // 默认本地确认窗口(字节)
		remoteWindowAckSize: 2500000,   // 默认远程确认窗口(字节)
		bandwidthLimitType:  0,         // 带宽限制类型：Hard limit
		slab:                NewSlab(), // 内存分配器
		rw:                  NewReadWriter(conn, bufSize),
		chunks:              make(map[uint32]*ChunkStream),
		option:              defaultOption,
	}
}

// InitSlab 初始化内存管理器的阈值
func (c *Conn) InitSlab(min, max int) {
	c.slab.Init(min, max)
}

// Read 在读锁内重组消息，处理控制消息后返回一条完整的上层消息。
func (c *Conn) Read(cs *ChunkStream) error {
	for {
		c.rLocker.Lock()

		// 只读不写
		ncs, err := c.getIntactChunk()
		if err != nil {
			c.rLocker.Unlock()
			return err
		}

		c.rLocker.Unlock()

		// 累计本轮接收的消息载荷字节数，达到对端确认窗口后发送 ACK。
		if err := c.ack(ncs.length); err != nil {
			return err
		}

		// 处理控制消息，消息不再传递给上层
		if !c.handleControlMsg(ncs) {
			*cs = *ncs
			return nil
		}
	}
}

// Write 在写锁内将完整消息分块写入缓冲区。
func (c *Conn) Write(cs *ChunkStream) error {
	if false {
		// 此 ACK 丢包分支当前禁用。
		ackSent := atomic.LoadUint32(&c.ackSent)

		// 为避免客户端一直接收不过来，这里采用了丢包的策略
		if c.sent > c.windowAckSize && c.sent > 3*ackSent {
			logging.Errorf("sent(%d)>3*ackSent(%d)", c.sent, ackSent)
			return nil
		}
	}

	c.wLocker.Lock()
	defer c.wLocker.Unlock()

	// 只写不读
	err := cs.writeChunk(c.rw, c.chunkSize)
	if err != nil {
		return err
	}

	// 发送量统计
	c.sent += cs.length
	if c.sent >= 0xf0000000 {
		c.sent = 0
	}

	return nil
}

// Flush 【写锁】刷新缓冲
func (c *Conn) Flush() error {
	c.wLocker.Lock()
	defer c.wLocker.Unlock()

	return c.rw.Flush()
}

// Close 关闭connection时要执行的工作
func (c *Conn) Close() error {
	err := c.Conn.Close()
	c.rLocker.Lock()
	defer c.rLocker.Unlock()
	if c.slab.stat.max > 0 || c.slab.stat.medium > 0 || c.slab.stat.min > 0 {
		logging.Tracef("Connection from %s is closed,slab stat,max:%d,medium:%d,min:%d",
			c.RemoteAddr(), c.slab.stat.max, c.slab.stat.medium, c.slab.stat.min)
	}
	return err
}

// 从网络缓冲中拼接chunk
func (c *Conn) getIntactChunk() (*ChunkStream, error) {
	var counter uint32

	// 释放上一条消息的载荷引用
	ncs, ok := c.chunks[c.csID]
	if ok {
		ncs.data = nil
	}

	for {
		// 放弃过长的chunk
		if counter*c.chunkSize > c.option.maxChunkSize {
			return nil, fmt.Errorf("chunk too large(single chunk %d > %d), discard it",
				counter*c.chunkSize, c.option.maxChunkSize)
		}

		// 读取 basic header 的首字节
		basicHeader, err := c.rw.ReadUintBE(1)
		if err != nil {
			return nil, err
		}

		// chunk basic header
		format := basicHeader >> 6 /* [0,3] */
		csID := basicHeader & 0x3f /* [0,63] */

		// 按 basic header 的 CSID 字段保存分块重组状态。
		ncs, ok = c.chunks[csID]
		if !ok {
			ncs = &ChunkStream{
				csid: csID,
			}
			c.chunks[csID] = ncs
		}

		// 块消息类型
		ncs.formatTmp = format

		// read chunk
		err = ncs.readChunk(c.rw, c.remoteChunkSize, c.slab)
		if err != nil {
			return nil, err
		}

		// store
		c.chunks[csID] = ncs

		// 整条消息重组完成
		if ncs.intact() {
			return ncs, nil
		}

		// 分片数
		counter++
	}
}

func (c *Conn) setChunkSize(cs *ChunkStream) {
	remoteChunkSize := binary.BigEndian.Uint32(cs.data)

	// 第一位必须是0
	if (remoteChunkSize >> 31) != 0 {
		logging.Error("incorrect control value when setting chunk size")
		return
	}

	// 当前接收策略拒绝小于 128 字节的 chunk size。
	if remoteChunkSize < 128 {
		logging.Error("incorrect chunk size ", remoteChunkSize)
		return
	}

	// 任何块不可能比消息(最大值0xFFFFFF)大
	if c.remoteChunkSize > 0xFFFFFF {
		remoteChunkSize = 0xFFFFFF
	}

	// 保存对端声明的块大小，供后续接收分块使用
	c.remoteChunkSize = remoteChunkSize
	logging.Trace("remote chunk size is changed to ", remoteChunkSize)
}

func (c *Conn) setPeerBandwidth(cs *ChunkStream) {
	bandwidth := binary.BigEndian.Uint32(cs.data)

	// 解析对端带宽窗口和限制类型；当前仅更新计数，不在此处执行限速。
	limitType := cs.data[4]

	// Dynamic: 已保存的限制类型为 Hard 时按 Hard 处理，否则不更新计数。
	if limitType == 2 && c.bandwidthLimitType == 0 {
		limitType = 0
	}

	// Hard and Soft limit
	switch limitType {
	case 0:
		// Hard: 将 ackReceived 设置为收到的窗口值。
		c.ackReceived = bandwidth
	case 1:
		// Soft: 将 ackReceived 收敛到收到的窗口值与当前计数中的较小值。
		if bandwidth < c.ackReceived {
			c.ackReceived = bandwidth
		}
	}

	logging.Trace("bandwidth is changed to ", bandwidth)
}

// 处理控制消息
func (c *Conn) handleControlMsg(cs *ChunkStream) bool {
	switch cs.typeID {
	case idSetChunkSize:
		c.setChunkSize(cs)
		return true
	case idAbortMessage:
		streamID := binary.BigEndian.Uint32(cs.data)

		/**
		当前仅记录 Abort Message，不处理其指定 chunk stream 的重组状态。
		*/
		logging.Errorf("ignore unrealize abort message, stream id=%d", streamID)
		return true
	case idAck:
		atomic.StoreUint32(&c.ackSent, binary.BigEndian.Uint32(cs.data))
		return true
	case idUser:
		c.handleUserMsg(cs)
		return true
	case idWindowAckSize:
		// 客户端或服务端发送该消息来通知对端发送确认消息所使用的视窗大小
		c.remoteWindowAckSize = binary.BigEndian.Uint32(cs.data)
		return true
	case idSetPeerBandwidth:
		c.setPeerBandwidth(cs)
		return true
	default:
		return false
	}
}

// 处理用户消息
func (c *Conn) handleUserMsg(cs *ChunkStream) {
	eventType := binary.BigEndian.Uint16(cs.data)
	switch eventType {
	case setBufferLen:
		// 事件数据的前4字节表示流ID,接下来的4字节表示缓冲区的大小(单位是毫秒)
		if len(cs.data) != 10 {
			logging.Errorf("setBufferLen command data(%d) != 10", len(cs.data))
			return
		}

		streamID := binary.BigEndian.Uint32(cs.data[2:6])
		bufferLen := binary.BigEndian.Uint32(cs.data[6:10])

		// bufferLen 是客户端请求的缓冲时长（毫秒），当前仅记录日志。
		logging.Tracef("unrealized setBufferLen command,streamID=%d,bufferLen=%d", streamID, bufferLen)
	case pingResponse:
		// 事件数据是客户端接收到pingRequest请求时的4字节时间戳
		if len(cs.data) != 6 {
			logging.Errorf("pingResponse command data(%d) != 6", len(cs.data))
			return
		}

		timestamp := binary.BigEndian.Uint32(cs.data[2:6])
		logging.Tracef("unrealized pingResponse command,timestamp=%d", timestamp)
	}
}

// 累计本轮接收的消息载荷字节数，达到对端确认窗口后发送 ACK。
func (c *Conn) ack(received uint32) error {
	c.received += received
	if c.received >= 0xf0000000 {
		c.received = 0
	}

	// 已接收但未确认的数据总数
	c.ackReceived += received
	if c.ackReceived >= c.remoteWindowAckSize {
		cs := c.NewAck(c.ackReceived)

		err := c.Write(cs)
		if err != nil {
			return err
		}

		c.ackReceived = 0
	}
	return nil
}
