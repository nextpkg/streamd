// Package core 数据漏斗: 数据单方面由上层写入，终端接收
package core

import (
	"sync"
	"time"

	"github.com/nextpkg/streamd/base"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/metrics"
	"github.com/nextpkg/streamd/packet"
	"sync/atomic"
)

var online atomic.Int64
var onlineMetrics sync.Mutex

func updateOnline(delta int64) {
	onlineMetrics.Lock()
	defer onlineMetrics.Unlock()
	metrics.GaugeGlobal(metrics.Playback, float64(online.Add(delta)))
}

// 必须大于1
const maxQueueLen = 4096

// Funnel 数据漏斗，数据单方面由上层写入，终端接收
type Funnel struct {
	Terminal

	// 数据统计
	*base.Active
	*base.Stat
	checkin int64

	// 流程控制
	wg     sync.WaitGroup
	status atomic.Bool

	// 数据通道
	pktChan      chan *packet.Packet
	queueMu      sync.Mutex
	aborted      atomic.Bool
	abortOnce    sync.Once
	drainTimer   *time.Timer
	drainTimeout time.Duration
}

// NewFunnel 终端执行顺序：Before() -> [loop]Write() -> After() -> Close()
func NewFunnel(t Terminal) *Funnel {
	ret := &Funnel{
		Terminal:     t,
		Active:       base.NewRwAlive(),
		Stat:         base.NewStat(),
		checkin:      time.Now().Unix(),
		pktChan:      make(chan *packet.Packet, maxQueueLen),
		drainTimeout: defaultOption.timeout,
	}

	ret.wg.Add(1)
	updateOnline(1)
	go ret.processor()

	return ret
}

// 处理流进来的数据
func (f *Funnel) processor() {
	f.Terminal.Before()
	defer func() {
		f.Terminal.After()
		f.queueMu.Lock()
		if f.drainTimer != nil {
			f.drainTimer.Stop()
		}
		for range f.pktChan {
		}
		f.queueMu.Unlock()
		f.abortOnce.Do(func() {})
		f.wg.Done()
	}()

	for p := range f.pktChan {
		if f.aborted.Load() {
			break
		}
		if p == nil {
			f.Active.RebaseAfterPresentation()
			if terminal, ok := f.Terminal.(interface{ Discontinuity() }); ok {
				terminal.Discontinuity()
			}
			continue
		}

		// 复制一个新的结构体，避免由多客户同时读导致的读写竞态
		pkt := *p

		// (公式: pts = dts + baseline)
		pkt.Baseline = pkt.TimeStamp + f.GetBaseTime()

		// 将数据传给客户端，如果客户端返回错误则认为是不可恢复的错误，直接关闭客户端
		err := f.Terminal.Write(&pkt)
		if err != nil {
			log.Debug(err)
			f.Close()
			break
		}

		// 更新状态信息
		f.Active.Keepalive()
		f.Active.SetMediaTime(&pkt)

		// 更新统计数据
		f.Stat.Update(&pkt)

		metrics.Average(f.Info().App, f.Info().Instance, metrics.PlaybackBW,
			float64(f.Stat.VideoBps()+f.Stat.AudioBps())*8,
		)
	}
}

func (f *Funnel) RebaseTime() {
	f.queueMu.Lock()
	if f.status.Load() {
		f.queueMu.Unlock()
		return
	}
	select {
	case f.pktChan <- nil:
		f.queueMu.Unlock()
	default:
		f.queueMu.Unlock()
		f.Abort()
	}
}

// Close 关闭
func (f *Funnel) Close() {
	f.queueMu.Lock()
	defer f.queueMu.Unlock()
	if !f.status.CompareAndSwap(false, true) {
		return
	}

	updateOnline(-1)

	close(f.pktChan)
	if _, interruptible := f.Terminal.(interface{ Abort() }); interruptible && !f.aborted.Load() {
		f.drainTimer = time.AfterFunc(f.drainTimeout, f.Abort)
	}
}

func (f *Funnel) Abort() {
	f.aborted.Store(true)
	f.Close()
	f.abortOnce.Do(func() {
		if terminal, ok := f.Terminal.(interface{ Abort() }); ok {
			terminal.Abort()
		}
	})
}

// Write 写数据
func (f *Funnel) Write(pkt *packet.Packet) error {
	f.queueMu.Lock()
	defer f.queueMu.Unlock()
	if f.status.Load() {
		return ErrWriterWasCanceled
	}

	select {
	case f.pktChan <- pkt:
		return nil
	default:
		return ErrQueueSaturated
	}
}

// Wait 等待关闭
func (f *Funnel) Wait() {
	f.wg.Wait()
}

func (f *Funnel) IsClosed() bool { return f.status.Load() }

func (f *Funnel) ViewerDeadline() time.Time {
	if !f.IsClosed() {
		if viewer, ok := f.Terminal.(interface{ ViewerDeadline() time.Time }); ok {
			return viewer.ViewerDeadline()
		}
	}
	return time.Time{}
}

// Checkin 登记时间
func (f *Funnel) Checkin() int64 {
	return f.checkin
}
