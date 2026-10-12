package rtmp

import (
	"net"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/rtmp/core"
	"github.com/stretchr/testify/assert"
)

func TestAddWriter(t *testing.T) {
	at := assert.New(t)

	cfg := config.Get()

	// 设置APP
	cfg.SetApp("t_app", &config.App{
		Enable:     true,
		AppName:    "t_app",
		StaticPush: nil,
		StaticPull: nil,
	})

	// 设置流描述信息
	info := base.NewInfo("t_app", "t_instance", false)

	i, _ := net.Pipe()

	reader := core.NewReader(core.NewConnServer(core.NewConn(i, core.ConnBufSize), 1024))

	s := NewStream(info)
	at.Nil(s.addReader(&StreamReader{reader}))

	_, o1 := net.Pipe()
	writer1 := core.NewWriter(core.NewConnClient("t_app", "t_ins", core.NewConn(o1, core.ConnBufSize)))

	// case1: 多个客户端启动，相同的客户端会有一个客户端会被断开

	at.Nil(s.addWriter(defaultStreamWriter(writer1)))
	at.NotNil(s.addWriter(defaultStreamWriter(writer1)))

	// 客户端数只有一个
	at.Equal(1, len(s.Writer()))

	// case2: 不同的客户端会被同时接受连接（以UID区分）
	_, o2 := net.Pipe()
	writer2 := core.NewWriter(core.NewConnClient("t_app", "t_ins", core.NewConn(o2, core.ConnBufSize)))
	at.Nil(s.addWriter(defaultStreamWriter(writer2)))

	// 客户端数一共是两个
	at.Equal(2, len(s.Writer()))

	// case3: 停止服务
	at.False(s.inside.control.IsDone())
	at.Nil(s.terminate(true))
	at.True(s.inside.control.IsDone())

	// 客户端剩余0个
	at.Equal(0, len(s.Writer()))
}

func TestNewStream(t *testing.T) {
	at := assert.New(t)

	d := flv.NewDemuxer()

	// 待测试I帧头
	p1 := &packet.Packet{
		Type: packet.PktVideo,
		Data: []byte{
			0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		},
	}
	at.Nil(d.Demux(p1))

	// 待测试I帧数据
	p2 := &packet.Packet{
		Type: packet.PktVideo,
		Data: []byte{
			0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0xd3, 0x65, 0x88, 0x80, 0x80, 0x03, 0x3f, 0xfe,
			0xf5, 0xf8, 0x45, 0x4f, 0x32, 0xcb, 0x1b, 0xb4,
			0x20, 0x3f, 0x85, 0x4d, 0xd6, 0x9b, 0xc2, 0xca,
			0x91, 0xb2, 0xbc, 0xe1, 0xfb, 0x35, 0x27, 0x44,
			0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00,
			0x03, 0x00, 0x00, 0x03, 0x00, 0x50, 0x99, 0x98,
			0x41, 0xd1, 0xaf, 0xd3, 0x24, 0xae, 0xa0, 0x00,
			0x00, 0x03, 0x00, 0x00, 0x0f, 0x60, 0x00, 0x11,
			0xc0, 0x00, 0x1b, 0x40, 0x00, 0x4e, 0x40, 0x01,
			0x1f, 0x00, 0x03, 0xb8, 0x00, 0x10, 0x80, 0x00,
			0x59, 0x00, 0x02, 0x38, 0x00, 0x0b, 0xe0, 0x00,
			0x5e, 0x00, 0x02, 0x20, 0x00, 0x11, 0x00, 0x00,
			0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
			0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03,
			0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00,
			0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
			0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03,
			0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00,
			0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
			0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03,
			0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00,
			0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
			0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03,
			0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00,
			0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
			0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03,
			0x00, 0x00, 0x40, 0x41,
		},
	}
	at.Nil(d.Demux(p2))

	// 设置APP
	cfg := config.Get()
	cfg.SetApp("t_app", &config.App{
		Enable:     true,
		AppName:    "t_app",
		StaticPush: nil,
		StaticPull: nil,
	})

	// 设置流描述信息
	info := base.NewInfo("t_app", "t_instance", false)
	s := NewStream(info)

	// 服务端
	i, o := net.Pipe()
	oConn := core.NewConn(o, 1)
	iConn := core.NewConn(i, 1)

	client := core.NewConnClient("t_app", "t_ins", oConn)
	server := core.NewConnServer(iConn, 1024)

	writer := core.NewWriter(client)
	reader := core.NewReader(server)

	err := s.addReader(&StreamReader{reader})
	at.Nil(err)

	// 客户端1
	i1, o1 := net.Pipe()
	o1Conn := core.NewConn(o1, 1)
	i1Conn := core.NewConn(i1, 1)

	client1 := core.NewConnClient("t_app", "t_ins", o1Conn)
	server1 := core.NewConnServer(i1Conn, 1024)

	writer1 := core.NewWriter(client1)
	reader1 := core.NewReader(server1)

	err = s.addWriter(defaultStreamWriter(writer1))
	at.Nil(err)

	// 客户端2
	i2, o2 := net.Pipe()
	o2Conn := core.NewConn(o2, 1)
	i2Conn := core.NewConn(i2, 1)

	client2 := core.NewConnClient("t_app", "t_ins", o2Conn)
	server2 := core.NewConnServer(i2Conn, 1024)

	writer2 := core.NewWriter(client2)
	reader2 := core.NewReader(server2)

	err = s.addWriter(defaultStreamWriter(writer2))
	at.Nil(err)

	//  case1: 写入一帧数据，应该会被两个客户端接收
	at.Nil(writer.Write(p1))

	// 第一个客户端收数据（p1）
	rp11 := &packet.Packet{}
	at.Nil(reader1.Read(rp11))
	at.Equal(p1.Data, rp11.Data)

	// 第二个客户端收数据（p1）
	rp21 := &packet.Packet{}
	at.Nil(reader2.Read(rp21))
	at.Equal(p1.Data, rp21.Data)

	at.Equal(2, len(s.Writer()))

	// case2: 模拟第二个客户端断线
	writer2.Close()

	// 再写入一次数据(p2)
	at.Nil(writer.Write(p2))
	time.Sleep(15 * time.Millisecond)

	// 第二个客户端接收不到数据了
	rp2 := &packet.Packet{}
	at.NotNil(reader2.Read(rp2))

	time.Sleep(15 * time.Millisecond)

	at.Equal(1, len(s.Writer()))
	return
}
