package core

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/nextpkg/goav/amf"
	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/internal/errutil"
	"github.com/nextpkg/streamd/internal/logging"
)

// ErrRspFailed 响应数据格式错误
var ErrRspFailed = errutil.New("invalid server response data")

// ConnClient RTMP客户端
type ConnClient struct {
	connect       base.ConnectInfo // 客户端参数
	publish       base.PublishInfo // 客户端参数
	current       string           // 通信：客户端正在执行的指令名称
	streamID      uint32           // 通信：客户端获得的流的ID
	conn          *Conn            // 通信：RTMP服务
	transactionID int              // 通信：事务ID
}

// NewConnClientByURL 使用RTMP链接获取RTMP客户端
func NewConnClientByURL(rtmpURL string, timeout time.Duration) *ConnClient {
	client, err := newConnClientByURL(context.Background(), rtmpURL, timeout)
	if err != nil {
		logging.Error(err)
		return nil
	}
	return client
}

type ClientOptions struct {
	BufferSize int
	SlabMin    int
	SlabMax    int
}

func DialConnClient(ctx context.Context, rtmpURL string, timeout time.Duration, options ClientOptions) (*ConnClient, error) {
	return dialConnClient(ctx, rtmpURL, timeout, options)
}

func newConnClientByURL(ctx context.Context, rtmpURL string, timeout time.Duration) (*ConnClient, error) {
	return dialConnClient(ctx, rtmpURL, timeout, ClientOptions{})
}

func dialConnClient(ctx context.Context, rtmpURL string, timeout time.Duration, options ClientOptions) (*ConnClient, error) {
	u, err := url.Parse(rtmpURL)
	if err != nil || len(u.Host) == 0 || u.Scheme != "rtmp" {
		return nil, errutil.New("invalid rtmp url,host or protocol")
	}

	address := u.Host
	if u.Port() == "" {
		address = net.JoinHostPort(u.Hostname(), "1935")
	}

	// path
	ps := strings.Split(strings.TrimLeft(u.Path, "/"), "/")
	if len(ps) != 2 || ps[0] == "" || ps[1] == "" {
		return nil, errutil.New("invalid rtmp path")
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, errutil.Wrapf(err, "Dial rtmp server failed,host='%s'", address)
	}

	bufferSize := options.BufferSize
	if bufferSize <= 0 {
		bufferSize = ConnBufSize
	}
	c := NewConn(conn, bufferSize)
	c.InitSlab(options.SlabMin, options.SlabMax)

	// RTMP服务
	return NewConnClient(ps[0], ps[1], c), nil
}

// NewConnClient RTMP客户端
func NewConnClient(app, instance string, conn *Conn) *ConnClient {
	return &ConnClient{
		conn: conn,
		connect: base.ConnectInfo{
			App:   app,
			TcURL: "rtmp://" + conn.LocalAddr().String() + "/" + app,
		},
		publish: base.PublishInfo{
			Name: instance,
			Type: publishLive,
		},
		transactionID: 1,
	}
}

// GetInfo 返回Info的字段值
func (c *ConnClient) GetInfo() (app string, instance string) {
	return c.connect.App, c.publish.Name
}

// GetPublish 获取发布流的信息
func (c *ConnClient) GetPublish() *base.PublishInfo {
	return &c.publish
}

// GetConnect 获取RTMP连接的信息
func (c *ConnClient) GetConnect() *base.ConnectInfo {
	return &c.connect
}

// ===================================

// Write 向服务端发送数据
func (c *ConnClient) Write(cs *ChunkStream) error {
	// 接收到chunk后，对metadata进行整理
	err := cs.unpack()
	if err != nil {
		return errutil.Wrap(err, "unpack and get data failed")
	}

	return c.conn.Write(cs)
}

// Read 从服务端读取数据
func (c *ConnClient) Read(cs *ChunkStream) error {
	return c.conn.Read(cs)
}

// Close 关闭连接管道
func (c *ConnClient) Close() error {
	return c.conn.Close()
}

// Flush 强制清空缓冲
func (c *ConnClient) Flush() error {
	return c.conn.Flush()
}

// ===================================

// Start 发起一个RTMP连接到服务端
func (c *ConnClient) Start(method string) error {
	// 1. 握手
	err := c.conn.HandshakeClient()
	if err != nil {
		return errutil.Wrap(err, "client handshake to server")
	}

	// 2. 交互 Connect
	err = c.Connect()
	if err != nil {
		return errutil.Wrap(err, "client send CONNECT command to server")
	}

	// 3. 交互 CreateStream
	err = c.CreateStream()
	if err != nil {
		return errutil.Wrap(err, "client send CREATE_STREAM command to server")
	}

	// 4. 开始 Play 或者 Publish
	switch method {
	case Publish:
		if err := c.Publish(); err != nil {
			return errutil.Wrap(err, "client send PUBLISH command to server")
		}
	case Play:
		if err := c.Play(); err != nil {
			return errutil.Wrap(err, "client send PLAY command to server")
		}
	default:
		return errutil.New("unknown method")
	}

	return nil
}

// StartPlay 播放流
func (c *ConnClient) StartPlay() error {
	return c.Start(Play)
}

// StartPublish 发布流
func (c *ConnClient) StartPublish() error {
	return c.Start(Publish)
}

// Connect 发送Connect命令并接收响应
func (c *ConnClient) Connect() error {
	// always set to 1
	c.transactionID = 1
	c.current = Connect

	event := make(amf.Object)
	event["app"] = c.connect.App
	event["flashVer"] = flashVer
	event["tcUrl"] = c.connect.TcURL

	// ready to request
	logging.Trace("Connect chunk size before sending is ", c.conn.chunkSize)

	err := c.sendCmdMsg(Connect, c.transactionID, event)
	if err != nil {
		return errutil.Wrapf(err, "sendCmdMsg,transaction id=%d,app='%s',tcUrl='%s'",
			c.transactionID, c.connect.App, c.connect.TcURL)
	}

	err = c.recvCmdMsg()
	if err != nil {
		return errutil.Wrapf(err, "recvCmdMsg,transaction id=%d,app='%s',tcUrl='%s'",
			c.transactionID, c.connect.App, c.connect.TcURL)
	}

	logging.Trace("Connect chunk size after sending is ", c.conn.chunkSize)

	return nil
}

// CreateStream 发送CreateStream命令并接收响应
func (c *ConnClient) CreateStream() error {
	c.transactionID++
	c.current = CreateStream

	// 格式: <command name>,<transaction id>,<command object>
	err := c.sendCmdMsg(CreateStream, c.transactionID, nil)
	if err != nil {
		return errutil.Wrapf(err, "sendCmdMsg,transaction id=%d", c.transactionID)
	}

	err = c.recvCmdMsg()
	if err != nil {
		return errutil.Wrapf(err, "recvCmdMsg,transaction id=%d", c.transactionID)
	}

	return nil
}

// Publish 发布类型: live
func (c *ConnClient) Publish() error {
	c.current = Publish
	c.transactionID++

	// 格式: <command name>,<transaction ID>,<command object>,<publishing name>,<publishing type>
	err := c.sendCmdMsg(Publish, 0, nil, c.publish.Name, publishLive)
	if err != nil {
		return errutil.Wrapf(err, "sendCmdMsg,transaction id=%d,instance='%s'", c.transactionID, c.publish.Name)
	}

	err = c.recvCmdMsg()
	if err != nil {
		return errutil.Wrapf(err, "recvCmdMsg,transaction id=%d,instance='%s'", c.transactionID, c.publish.Name)
	}

	return nil
}

// Play 直播
func (c *ConnClient) Play() error {
	c.current = Play
	c.transactionID++

	// 格式: <command name>,<transaction id>,<command object>,
	// <stream name>,<Start(可选) float>,<Duration(可选) float>,<Reset(可选) bool>
	err := c.sendCmdMsg(Play, 0, nil, c.publish.Name)
	if err != nil {
		return errutil.Wrapf(err, "sendCmdMsg,transaction id=%d,instance='%s'", c.transactionID, c.publish.Name)
	}

	err = c.recvCmdMsg()
	if err != nil {
		return errutil.Wrapf(err, "recvCmdMsg,transaction id=%d,instance='%s'", c.transactionID, c.publish.Name)
	}

	return nil
}

func (c *ConnClient) strCmd(v interface{}) error {
	str := v.(string)

	switch c.current {
	case Connect, CreateStream:
		if str != respResult {
			return fmt.Errorf("connect or createStream command response result='%s'", str)
		}
	case Publish:
		if str != onStatus {
			return ErrRspFailed
		}
	}

	return nil
}

func (c *ConnClient) intCmd(k int, v interface{}) error {
	id := int(v.(float64))

	switch c.current {
	case Connect, CreateStream:
		switch k {
		case 1:
			if c.transactionID != id {
				return ErrRspFailed
			}
		case 3:
			c.streamID = uint32(id)
		}
	case Publish:
		if id != 0 {
			return ErrRspFailed
		}
	}

	return nil
}

func (c *ConnClient) objCmd(v interface{}) error {
	property := v.(amf.Object)

	switch c.current {
	case Connect:
		// RTMP 服务端版本信息
		_, ok := property["fmsVer"]
		if ok {
			return nil
		}

		// event
		code, ok := property["code"]
		if ok {
			str := code.(string)
			if str != codeConnectSuccess {
				return ErrRspFailed
			}

			return nil
		}

		return ErrRspFailed
	case Publish:

		// event
		code, ok := property["code"]
		if !ok {
			return ErrRspFailed
		}

		str := code.(string)
		if str != codePublishStart {
			return fmt.Errorf("server return publish code='%s'", code.(string))
		}
	}

	return nil
}

// 验证服务端响应的指令消息
func (c *ConnClient) recvCmdMsg() error {
	for {
		cs := &ChunkStream{}

		err := c.conn.Read(cs)
		if err != nil {
			return err
		}

		var cmd []interface{}

		// 只处理指令消息
		// 客户端发送这些消息来完成连接、创建流、发布、播放、暂停等操作
		// 像状态、结果这样的指令消息，用于通知发送方请求的指令状态
		switch cs.typeID {
		case 20:
			cmd, err = amf.NewEnDecAMF0().DecodeBatch(bytes.NewReader(cs.data))
			if err != nil {
				return errutil.Wrap(err, "decode amf0")
			}
		case 17:
			cmd, err = amf.NewEnDecAMF3().DecodeBatch(bytes.NewReader(cs.data))
			if err != nil {
				return errutil.Wrap(err, "decode amf3")
			}
		default:
			continue
		}

		for k, v := range cmd {
			switch v.(type) {
			case string:
				if err := c.strCmd(v); err != nil {
					return errutil.Wrap(err, "handle str command")
				}
			case float64:
				if err := c.intCmd(k, v); err != nil {
					return errutil.Wrap(err, "handle int command")
				}
			case amf.Object:
				if err := c.objCmd(v); err != nil {
					return errutil.Wrap(err, "handle obj command")
				}
			}
		}

		return nil
	}
}

// 构造并发送命令消息，CSID:3，消息类型: 20
func (c *ConnClient) sendCmdMsg(args ...interface{}) error {
	command := bytes.NewBuffer(nil)

	// 使用AMF0编码
	err := amf.NewEnDecAMF0().EncodeBatch(command, args...)
	if err != nil {
		return errutil.Wrap(err, "encode command message")
	}

	msg := command.Bytes()

	// 构造消息
	cs := &ChunkStream{
		format:    0,
		csid:      3, // 命令消息所在通道号
		timestamp: 0,
		typeID:    20, // 消息类型20代表AMF0编码, 消息类型17代表AMF3编码
		streamID:  c.streamID,
		length:    uint32(len(msg)),
		data:      msg,
	}

	if err := c.conn.Write(cs); err != nil {
		return errutil.Wrap(err, "send command message to server")
	}

	return c.conn.Flush()
}
