package base

import (
	"math"
	"strconv"

	log "github.com/nextpkg/streamd/internal/logging"
)

// Fragment 分片
type Fragment struct {
	index    int64
	initTime uint32 // 第一次update的时间: ms
	lastTime int64  // 最后一次update的时间: ms
	started  bool
}

// NewFragment Ts分片时间辅助，ratedLen的单位是秒
func NewFragment(ratedLen int64) *Fragment {
	return &Fragment{
		lastTime: ratedLen * 1000,
	}
}

func (f *Fragment) isNewer(t1, t2 uint32) bool {
	const bp uint32 = math.MaxUint32>>1 + 1

	if t1-t2 == bp {
		return t1 > t2
	}

	return t1 != t2 && t1-t2 < bp
}

// Update 更新分片的最后活跃时间戳
func (f *Fragment) Update(ts uint32) {
	if ts < 0 {
		// rtmp的ts是uint32类型，不会走到这个分支
		log.Warn("update ts should not <0")
		return
	}

	// 分片时间初始化
	if !f.started {
		f.started = true
		f.initTime = ts
		f.lastTime = int64(ts)
		return
	}

	if ts == f.initTime && f.lastTime == int64(ts) {
		return
	}

	// 发生时钟异常
	if !f.isNewer(ts, f.initTime) {
		log.Warn("Not continuous timestamp")
		return
	}

	f.lastTime = int64(ts)
}

// Create 新起一个分片记录
func (f *Fragment) Create() {
	f.index++

	f.initTime = 0
	f.lastTime = 0
	f.started = false
}

// Duration 分片的时长, 单位: ms
func (f *Fragment) Duration() int64 {
	gap := f.lastTime - int64(f.initTime)
	if gap < 0 {
		gap += math.MaxUint32 + 1
	}

	return gap
}

func (fragment *Fragment) DurationAt(timestamp uint32) int64 {
	if !fragment.started {
		return fragment.Duration()
	}
	if !fragment.isNewer(timestamp, fragment.initTime) {
		return 0
	}
	return int64(timestamp - fragment.initTime)
}

// Index 序号
func (f *Fragment) Index() int64 {
	return f.index
}

// Sequence 在这里规定序号的规则
func (f *Fragment) Sequence() string {
	return strconv.FormatInt(f.index, 10)
}
