package core

import (
	"fmt"

	"github.com/nextpkg/goav/amf"
	errors "github.com/nextpkg/streamd/internal/errutil"
	log "github.com/nextpkg/streamd/internal/logging"
)

// 客户端向服务器发送 publish 命令用来发布带有名字的流。
// 任何客户端都可以使用这个 名字播放这个流和接收发布的音频，视频和数据信息
// 格式: <命令名称: publish>,<事务ID: 0>,<命令对象: nil>,<发布流名称>,<发布流类型>
func (s *ConnServer) handlePublish(args []interface{}) error {

	for k, v := range args {
		switch v := v.(type) {
		case float64:
			s.transactionID = uint32(v)
		case amf.Object:
		case string:
			switch k {
			case 2:

				// 发布流的名称
				s.publish.Name = v
				log.Trace("publish name is ", s.publish.Name)
			case 3:
				// 保存客户端声明的发布类型；录制行为由应用的 recorder 配置控制。
				switch v {
				case publishLive, publishRecord, publishAppend:
					s.publish.Type = v
				default:
					return errors.New("invalid publish type")
				}
			}
		}
	}

	return nil
}

// 客户端发送个命令给服务器用以播放一个流。
// 格式: <命令名称: play>,<事务ID: 0>,<命令对象: nil>,<流名称: string>,<起始时间: number>,<可选回放时间段: number>
func (s *ConnServer) handlePlay(args []interface{}) error {

	for k, v := range args {
		switch v := v.(type) {
		case float64:
			switch k {
			case 0:
				// 事务的 ID 设置为 0
				s.transactionID = uint32(v)
			case 3:
				// start: 一个可选的参数,它指定了起始的时间以秒为单位
				start := int(v)

				switch {
				case start <= -2:
					// 当前仅记录该 start 值，不执行直播转录播查找。
					log.Tracef("unrealized start=%d", start)
				case start == -1, start == 0:
					// 使用直播播放路径。
				case start > 0:
					// 当前仅记录该 start 值，不执行录播定位。
					log.Tracef("unrealized start=%d", start)
				}
			case 4:
				// duration: 一个可选的参数，它指定了回放的持续时间以秒为单位
				duration := int(v)

				switch {
				case duration <= -1:
					// 不设置播放时长限制。
				case duration == 0:
					// 当前仅记录零时长请求，不执行单帧播放。
					log.Tracef("unrealized duration=%d", duration)
				case duration > 0:
					// 当前仅记录指定时长，不按 duration 截断播放。
					log.Tracef("unrealized duration=%d", duration)
				}
			}
		case amf.Object:
		case string:
			// 需要播放的流的名字
			s.publish.Name = v
		case bool:
			// 一个可选的布尔值或布尔数字，指定示是否刷新之前的播放列表
			log.Tracef("unrealized play:reset")
		}
	}

	return nil
}

// 获取指定视频的长度
// 格式: <命令名称: getStreamLength>,<事务ID: number>,<发布流名称>
func (s *ConnServer) handleGetStreamLength(args []interface{}) error {

	for _, v := range args {
		switch v := v.(type) {
		case float64:
			s.transactionID = uint32(v)
		case string:

			// 当前 getStreamLength 固定返回零。
			s.duration = 0
		}
	}

	return nil
}

// 客户端发送这个命令去创建一个用于消息通信的逻辑通道
// 音频、视频和metadata都是通过使用 createStream 命令创建的流通道进行传输的
// 格式: <命令名称: createStream>,<事务ID: number>,<命令对象: object>
func (s *ConnServer) handleCreateStream(args []interface{}) error {

	for _, v := range args {
		switch v := v.(type) {
		case float64:
			s.transactionID = uint32(v)
		case amf.Object:

			// 客户端自定义命令对象，当前仅记录日志。
			log.Trace("unrealized object")
		}
	}

	return nil
}

func (s *ConnServer) connectCommandMsg(v interface{}) {

	property := v.(amf.Object)

	// 客户端连接到服务器上的应用实例名称
	app, ok := property["app"]
	if ok {
		s.connect.App = app.(string)
	}

	// Flash 播放器版本
	flashVer, ok := property["flashVer"]
	if ok {
		s.connect.FlashVer = flashVer.(string)
	}

	// 当前连接的 SWF 源文件地址
	swfURL, ok := property["swfUrl"]
	if ok {
		s.connect.SwfURL = swfURL.(string)
	}

	// 服务端的地址
	tcURL, ok := property["tcUrl"]
	if ok {
		s.connect.TcURL = tcURL.(string)
	}

	// 如果使用代理设置为 True
	fpad, ok := property["fpad"]
	if ok {
		s.connect.FPad = fpad.(bool)
	}

	// 表明客户端支持的音频编码
	audioCodecs, ok := property["audioCodecs"]
	if ok {
		s.connect.AudioCodecs = int(audioCodecs.(float64))
	}

	// 表明客户端支持的视频频编码
	videoCodecs, ok := property["videoCodecs"]
	if ok {
		s.connect.VideoCodecs = int(videoCodecs.(float64))
	}

	// 表明支持的特殊的视频函数
	videoFunction, ok := property["videoFunction"]
	if ok {
		s.connect.VideoFunction = int(videoFunction.(float64))
	}

	// 加载 SWF 文件的网页地址
	pageURL, ok := property["pageUrl"]
	if ok {
		s.connect.PageURL = pageURL.(string)
	}

	// AMF 编码方法
	objectEncoding, ok := property["objectEncoding"]
	if ok {
		s.connect.ObjectEncoding = int(objectEncoding.(float64))
	}
}

// 解析客户端发过来的connect指令
// 客户端发送连接命令给服务器去请求连接服务器上的一个应用实例
// 消息格式: <命令名称: connect>,<事务ID: number>,<命令对象: object>,<可选用户参数: object>
func (s *ConnServer) handleConnect(args []interface{}) error {

	for k, v := range args {
		switch v := v.(type) {
		case float64:

			// 事务ID
			id := int(v)
			if id != 1 {
				return fmt.Errorf("invalid transaction ID=%d", id)
			}

			s.transactionID = 1
		case amf.Object:

			// 命令对象
			if k == 1 {

				s.connectCommandMsg(v)
				continue
			}

			// 用户可选参数，当前仅记录日志。
			log.Tracef("unrealized optional user object")
		}
	}

	return nil
}

// 解析客户端发过来的fcUnpublish指令
// 消息格式: <命令名称: FCUnpublish>,<事务ID: number>,<命令对象: nil>,<流名称: string>
func (s *ConnServer) handleFcunpublish(args []interface{}) error {

	for _, v := range args {
		switch v := v.(type) {
		case float64:
			s.transactionID = uint32(v)
		case amf.Object:
		case string:
			publishName := v
			if publishName != s.publish.Name {
				return fmt.Errorf("invalid publish name=%s", publishName)
			}
		}
	}

	return nil
}

// 解析客户端发过来的deleteStream指令，不需要响应客户端
// 格式：<命令名称: deleteStream>,<事务ID: 0>,<命令对象: nil>,<流ID: number>
func (s *ConnServer) handleDeleteStream(args []interface{}) error {

	for k, v := range args {
		switch v := v.(type) {
		case float64:
			num := uint32(v)

			switch k {
			case 0:
				// 事务ID
				transactionID := num
				if transactionID != 0 {
					return errors.New("invalid request data")
				}
			case 2:
				// 流ID
				streamID := num
				if streamID != s.streamID {
					return errors.New("invalid stream ID")
				}
			}
		case amf.Object:
		}
	}

	return nil
}
