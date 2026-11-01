package rtmp

import (
	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
)

const cacheTTL = 60

// Handler 读写注册接口
type Handler interface {
	InstallReader(base.ReadCloser)
	InstallWriter(base.WriteCloser)
	Close()
}

// EmbedWriter 内嵌客户端
type EmbedWriter interface {
	GetWriter(*base.Info, *base.ConnectInfo) (base.WriteCloser, error)
}

// StreamWriter 消费者
type StreamWriter struct {
	base.WriteCloser
	hasGOP bool // true：已经发送了GOP包，false反之
}

// 默认的消费者
func defaultStreamWriter(w base.WriteCloser) *StreamWriter {
	return &StreamWriter{
		WriteCloser: w,
		hasGOP:      false,
	}
}

// StreamReader 生产者
type StreamReader struct {
	base.ReadCloser
}

// 源站的状态
type sourceStatus struct {
	usable   bool   // 源站是否可用
	deadTime int64  // 源站失效的时间
	pullHost string // 源站的位置
}

// 管理源站列表
type sourceCache struct {
	data   []*sourceStatus // 源站列表
	time   int64           // 列表创建的 Unix 时间，单位：秒
	config *config.Config
	app    *config.App
}
