package base

import (
	"time"

	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/packet"
	"sync/atomic"
)

// Active 用来辅助读写的超时处理
type Active struct {
	active               atomic.Int64
	baseMediaTime        atomic.Uint32
	lastVideoTime        atomic.Uint32
	lastAudioTime        atomic.Uint32
	lastPresentationTime atomic.Uint32
	videoInterval        atomic.Uint32
	audioInterval        atomic.Uint32
}

// NewRwAlive 用来辅助读写的超时处理
func NewRwAlive() *Active {
	ra := &Active{}
	ra.active.Store(time.Now().Unix())
	return ra
}

// GetBaseTime 获取基准时间戳【migrate会用到】
func (rw *Active) GetBaseTime() uint32 {
	return rw.baseMediaTime.Load()
}

// RebaseTime [写时]设置基准时间戳: 以音频/视频时间戳数值大的一个作为基准时间戳
func (rw *Active) RebaseTime() {
	if rw.lastAudioTime.Load() > rw.lastVideoTime.Load() {
		rw.baseMediaTime.Store(rw.lastAudioTime.Load())
	} else {
		rw.baseMediaTime.Store(rw.lastVideoTime.Load())
	}
}

func (rw *Active) RebaseAfterPresentation() {
	last := max(rw.lastAudioTime.Load(), rw.lastVideoTime.Load(), rw.lastPresentationTime.Load())
	interval := max(rw.videoInterval.Load(), rw.audioInterval.Load(), 1)
	rw.baseMediaTime.Store(last + interval)
}

// SetMediaTime [写时]设置音视频的最后活跃时间戳(因为RTMP包有增量时间戳)
func (rw *Active) SetMediaTime(p *packet.Packet) {
	switch p.Type {
	case packet.PktVideo:
		if header, ok := p.Header.(packet.VideoPacketHeader); ok {
			if header.IsSeqHdr() || header.IsEndOfSeq() {
				return
			}
			presentation := int64(p.Baseline) + int64(header.CompositionTime())
			if presentation > int64(rw.lastPresentationTime.Load()) {
				rw.lastPresentationTime.Store(uint32(presentation))
			}
		}
		previous := rw.lastVideoTime.Swap(p.Baseline)
		if previous != 0 && p.Baseline > previous {
			rw.videoInterval.Store(p.Baseline - previous)
		}
	case packet.PktAudio:
		if header, ok := p.Header.(packet.AudioPacketHeader); ok && header.IsAACSeqHdr() {
			return
		}
		previous := rw.lastAudioTime.Swap(p.Baseline)
		if previous != 0 && p.Baseline > previous {
			rw.audioInterval.Store(p.Baseline - previous)
		}
	}
}

// LastVideoTime is last active video timestamp
func (rw *Active) LastVideoTime() uint32 {
	return rw.lastVideoTime.Load()
}

// LastAudioTime is last active audio timestamp
func (rw *Active) LastAudioTime() uint32 {
	return rw.lastAudioTime.Load()
}

// Keepalive 保持活跃状态（当前时间）
func (rw *Active) Keepalive() {
	rw.active.Store(time.Now().Unix())
}

// IsTimeout 根据时间判断是否活跃, 活跃标准: 当前时间 - preTime < timeout
func (rw *Active) IsTimeout() bool {
	tmo := readTimeout.Load()
	if tmo <= 0 {
		return false
	}

	elapse := time.Now().Unix() - rw.active.Load()
	if elapse <= tmo {
		return false
	}

	log.Errorf("rtmp IO timeout. elapse=%ds. limit=%ds", elapse, tmo)
	return true
}

var readTimeout atomic.Int64

func SetReadTimeout(timeout time.Duration) {
	readTimeout.Store(int64(timeout / time.Second))
}
