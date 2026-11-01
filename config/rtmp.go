package config

import (
	"fmt"
	"sync/atomic"

	errors "github.com/nextpkg/streamd/internal/errutil"
)

type slabSettings struct{ minimum, maximum int }

var slabOverride atomic.Pointer[slabSettings]

func SetSlabOverrides(minimum, maximum int) error {
	if minimum < 0 || maximum < 0 || (maximum > 0 && minimum > maximum) {
		return errors.New("slab overrides require nonnegative sizes with minimum <= maximum")
	}
	slabOverride.Store(&slabSettings{minimum: minimum, maximum: maximum})
	return nil
}

// RTMP RTMP直播功能
type RTMP struct {
	Addr string `json:"addr"`
	// Gop 缓存的 GOP 数量；0 表示不缓存 GOP，新订阅者需等待后续关键帧。
	Gop         int    `json:"gop"`
	ChunkSize   uint32 `json:"chunk_size"`   // RTMP协议的chunk大小，单位：字节，0表示使用默认值
	SlabMin     int    `json:"slab_min"`     // 内存分配器的碎片最小值，0表示使用默认值
	SlabMax     int    `json:"slab_max"`     // 内存分配器的碎片最大值，0表示使用默认值
	BufSize     int    `json:"buf_size"`     // 网络收发包的缓冲大小，单位：字节
	EdgeAlive   int64  `json:"edge_alive"`   // 空闲连接（publisher）保持时间，单位，秒
	EdgeCheck   int64  `json:"edge_check"`   // 客户端连接（player）轮询检查间隔，单位，秒
	ReadTimeout int64  `json:"read_timeout"` // 服务端读IO超时时间，单位，秒
	DialTimeout int64  `json:"dial_timeout"` // 客户端与服务端的连接超时时间，单位，秒
}

func (r *RTMP) validate() error {
	if len(r.Addr) == 0 {
		return errors.New("rtmp: empty `addr`,`port` recommended value is 0.0.0.0:1935")
	}

	if r.BufSize < 128 {
		return fmt.Errorf("rtmp: `buf_size`<128,`buf_size` recommended value is 4096.`buf_size`=%d now",
			r.BufSize)
	}

	if r.ReadTimeout <= 0 {
		return errors.New("rtmp: `io_timeout`<=0")
	}

	if r.ChunkSize < 128 || r.ChunkSize > 12288 {
		return fmt.Errorf("rtmp: `chunk_size`<128||chunk_size`>12288,"+
			"`chunk_size` recommended value is 4096.`chunk_size`=%d now", r.ChunkSize)
	}

	if r.EdgeCheck <= 0 {
		return errors.New("rtmp: `edge_check`<=0,`edge_check` recommended value is 30")
	}

	if r.EdgeAlive <= 0 {
		return errors.New("rtmp: `edge_alive`<=0,`edge_alive` recommended value is 60(2*`edge_check`)")
	}

	if r.EdgeAlive <= r.EdgeCheck {
		return errors.New("rtmp: `edge_alive`<=`edge_check`," +
			"recommend increase `edge_alive` or decrease `edge_check`")
	}

	if r.Gop < 0 {
		return errors.New("rtmp: `gop`<0,`gop` recommended value is 1")
	}

	if override := slabOverride.Load(); override != nil {
		if override.minimum > 0 {
			r.SlabMin = override.minimum
		}
		if override.maximum > 0 {
			r.SlabMax = override.maximum
		}
	}
	maximum := r.SlabMax
	if maximum == 0 {
		maximum = 256
	}
	maxKilobytes := int(^uint(0)>>1) / 1024
	if r.SlabMin < 0 || r.SlabMax < 0 || r.SlabMin > maximum || maximum > maxKilobytes {
		return errors.New("invalid slab allocation thresholds in KiB")
	}

	return nil
}
