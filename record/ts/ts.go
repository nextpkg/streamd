package ts

import (
	"bytes"

	"github.com/nextpkg/goav/amf"
	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/container/ts"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/record/storage"
	"github.com/nextpkg/streamd/rtmp/core"
)

// cache TS元数据
type cache struct {
	isKeyframe  bool
	composition uint32
	splitPTS    uint32
}

// Stream 将流录制为TS格式, 每隔一段时间会切片
type Stream struct {
	*core.Universal

	store        *storage.Storage
	ts           *bytes.Buffer
	mixer        *ts.Mixer
	cache        *cache
	resumeOffset uint32
}

// NewStream 初始化TS录制器
func NewStream(info *base.Info, store *storage.Storage) *core.Funnel {
	return core.NewFunnel(NewTerminal(info, store))
}

func NewTerminal(info *base.Info, store *storage.Storage) *Stream {
	buffer := bytes.NewBuffer(nil)

	ret := &Stream{
		Universal: core.NewUniversal(info),
		store:     store,
		ts:        buffer,
		mixer:     ts.NewMixer(buffer),
		cache:     &cache{},
	}

	return ret
}

// Write 调用此函数, 从管道内读取数据并处理
func (s *Stream) Write(p *packet.Packet) error {
	if !s.unpack(p) {
		return nil
	}
	if !s.prepare(false) {
		return nil
	}

	if s.cache.isKeyframe {
		err := s.store.WriteAt(p.Baseline, s.ts.Bytes(), s.mixer.NextTimestamp())
		if err != nil {
			// 不返回错误，避免存储故障导致实时视频故障
			log.Error(err)
			return nil
		}

		// 切割TS，从关键帧开始切文件(含序列头)
		if s.store.CanSplit() {
			if !s.prepare(true) {
				return nil
			}
			s.cache.splitPTS = p.Baseline
			s.store.UpdateTime(p.Baseline)
		} else {
			s.ts.Reset()
		}
	}

	pktTS := p.Baseline - s.cache.splitPTS + s.resumeOffset
	avcTS := s.cache.composition

	err := s.mixer.Update(p, pktTS, avcTS)
	if err != nil {
		log.Error(err)
		return nil
	}

	err = s.mixer.Mux(p)
	if err != nil {
		log.Error(err)
		return nil
	}

	return nil
}

// After 退出后处理流程
func (s *Stream) After() {
	defer s.store.Release()

	err := s.store.Write(s.ts.Bytes(), s.mixer.NextTimestamp())
	if err != nil {
		log.Error(err)
	}

	// 弱网环境下，block=true可能会导致同个服务有多个相同的recorder实例，引起录制混乱
	// 遇到类似问题可优先解决网络问题，降低单服务器负载或提高存储服务能力
	//
	// block=false会导致文件还没被正常关闭，进程就退出了，可能会导致文件缺失
	err = s.store.Close(true)
	if err != nil {
		log.Error(err)
	}
}

func (s *Stream) Finish() error {
	s.store.ForceCompletion()
	s.After()
	return s.store.Error()
}

// 分离FLV和数据
func (s *Stream) unpack(p *packet.Packet) bool {
	s.cache.isKeyframe = false
	header := p.Header

	switch p.Type {
	case packet.PktVideo:
		vh := header.(packet.VideoPacketHeader)

		// 暂只支持H264
		if !vh.IsCodecAvc() {
			log.Errorf("support H264 only,key='%s',code id=%d", s.Info().Key, vh.CodecID())
			return false
		}

		if vh.IsSeqHdr() {
			err := s.mixer.SaveAVCHeader(p)
			if err != nil {
				log.Error(err)
			}

			return false
		}

		if vh.IsEndOfSeq() {
			return false
		}

		s.cache.isKeyframe = vh.IsKeyFrame()
		s.cache.composition = uint32(vh.CompositionTime())
	case packet.PktAudio:
		ah := header.(packet.AudioPacketHeader)

		// 只支持aac
		if !ah.IsSoundAAC() {
			log.Errorf("support AAC only,key='%s',sound format=%d", s.Info().Key, ah.SoundFormat())
			return false
		}

		if ah.IsAACSeqHdr() {
			err := s.mixer.SaveAACHeader(p)
			if err != nil {
				log.Error(err)
			}

			return false
		}
	case packet.PktMetadata:
		// 忽略所有的"metadata", 以免造成"metadata"污染
		return false
	}

	return true
}

// 打开文件并且初始化TS头
func (s *Stream) prepare(canSplit bool) bool {
	if s.store.IsOpened() {
		if !canSplit {
			return true
		}

		// 此处block=false有利于快速生成新的分片，但如果此时进程退出将可能导致有部分碎片未能及时写入存储，因此可能会有数据丢失
		err := s.store.Close(false)
		if err != nil {
			log.Error(err)
			return false
		}
	}

	if !s.cache.isKeyframe {
		return false
	}

	s.ts.Reset()

	err := s.store.Open()
	if err != nil {
		if !s.store.WaitingFinalization() {
			log.Error(err)
		}
		return false
	}

	cfg := config.Get().Metadata

	obj := amf.Object{
		"Provider": cfg.Provider,
		"Service":  cfg.Service,
	}

	err = s.mixer.SaveMetadata(obj)
	if err != nil {
		log.Error(err)
	}

	s.resumeOffset, err = s.store.Resume(s.mixer.Resume, s.mixer.ResumeAt)
	if err != nil {
		log.Error(err)
		return false
	}

	return true
}

// Name writer's name
func (s *Stream) Name() string {
	return "ts"
}
