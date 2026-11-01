package rtmp

import (
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/rtmp/core"
	"time"

	"github.com/nextpkg/streamd/internal/errutil"
)

// Client 客户端
type Client struct {
	handler Handler
	players []EmbedWriter
}

// NewClient 客户端
func NewClient(h Handler, w ...EmbedWriter) *Client {
	return &Client{
		handler: h,
		players: w,
	}
}

// GetHandler 处理函数
func (c *Client) GetHandler() Handler {
	return c.handler
}

// dial 启动客户端连接
func (c *Client) dial(url, method string) error {
	conn := core.NewConnClientByURL(url, time.Duration(config.Get().RTMP.DialTimeout)*time.Second)
	if conn == nil {
		return errutil.New("init client connection failed")
	}

	// 客户端
	err := conn.Start(method)
	if err != nil {
		_ = conn.Close()
		return errutil.Wrap(err, "client start failed")
	}

	switch method {
	case core.Publish:
		w := core.NewWriter(conn)
		c.handler.InstallWriter(w)
	case core.Play:
		r := core.NewReader(conn)
		c.handler.InstallReader(r)

		// 加载更多player
		for _, player := range c.players {
			if player == nil {
				continue
			}

			writer, err := player.GetWriter(r.Info(), conn.GetConnect())
			if err != nil {
				return err
			}
			if writer == nil {
				continue
			}

			c.handler.InstallWriter(writer)
		}
	}

	return nil
}

// Play 使用Play指令启动客户端
func (c *Client) Play(url string) error {
	return c.dial(url, core.Play)
}

// Publish 使用Publish指令启动客户端
func (c *Client) Publish(url string) error {
	return c.dial(url, core.Publish)
}
