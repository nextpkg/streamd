package control

import (
	"context"

	"sync/atomic"
)

// Control 保存流程状态和取消通知。
type Control struct {
	ctx    context.Context
	cancel context.CancelFunc
	isDone atomic.Bool // 供 IsDone 读取的原子状态标记
}

// NewControl 保存流程状态和取消通知。
// isDone 仅设置 IsDone 的初始值；Done 通道在 Cancel 后关闭。
func NewControl(isDone bool) *Control {
	ctx, cancel := context.WithCancel(context.Background())

	ret := &Control{
		ctx:    ctx,
		cancel: cancel,
	}

	ret.isDone.Store(isDone)
	return ret
}

// Restart 重置状态为“进行中”
func (c *Control) Restart() {
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.isDone.Store(false)
}

// Cancel 取消
func (c *Control) Cancel() {
	c.isDone.Store(true)
	c.cancel()
}

// IsDone 是否在“已取消”状态
func (c *Control) IsDone() bool {
	return c.isDone.Load()
}

// Done 返回取消通知通道，调用者可接收该通道等待取消。
func (c *Control) Done() <-chan struct{} {
	return c.ctx.Done()
}
