// Package flv 将流录制为FLV格式, 每隔一段时间会切片
package flv

import (
	"bytes"

	"github.com/nextpkg/goav/amf"
	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/container/flv"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/record/storage"
	"github.com/nextpkg/streamd/rtmp/core"
)

// cache FLV元数据
type cache struct {
	isKeyframe bool
	splitPTS   uint32
}

// Stream 将流录制为FLV格式, 每隔一段时间会切片
type Stream struct {
	*core.Universal
	store           *storage.Storage
	flv             *bytes.Buffer
	mixer           *flv.Mixer
	cache           *cache
	videoHeaderSeen bool
	audioHeaderSeen bool
	resumeOffset    uint32
}

// NewStream 初始化FLV录制器
func NewStream(info *base.Info, store *storage.Storage) *core.Funnel {
	return core.NewFunnel(NewTerminal(info, store))
}

func NewTerminal(info *base.Info, store *storage.Storage) *Stream {
	buffer := bytes.NewBuffer(nil)
	ret := &Stream{
		Universal: core.NewUniversal(info),
		store:     store,
		flv:       buffer,
		mixer:     flv.NewMixer(buffer),
		cache:     &cache{},
	}
	return ret
}

// Write 读取数据包，处理数据
func (s *Stream) Write(p *packet.Packet) error {
	if !s.unpack(p) {
		return nil
	}
	if !s.prepare(false) {
		return nil
	}

	if s.cache.isKeyframe {
		err := s.store.WriteAt(p.Baseline, s.flv.Bytes(), s.mixer.NextTimestamp())
		if err != nil {
			// 不返回错误，避免存储故障导致实时视频故障
			log.Error(err)
			return nil
		}

		// 切割FLV，从关键帧开始切文件(含序列头)
		if s.store.CanSplit() {
			if !s.prepare(true) {
				return nil
			}
			s.cache.splitPTS = p.Baseline
			s.store.UpdateTime(p.Baseline)
		} else {
			s.flv.Reset()
		}
	}

	// 复用开头含有关键帧的数据，将中间数据放在变量中, 一次性再写入文件中可以减少系统调用次数
	err := s.mixer.Mux(p, p.Baseline-s.cache.splitPTS+s.resumeOffset)
	if err != nil {
		log.Error(err)
	}

	return nil
}

// After 退出后处理流程
func (s *Stream) After() {
	defer s.store.Release()
	err := s.store.Write(s.flv.Bytes(), s.mixer.NextTimestamp())
	if err != nil {
		log.Error(err)
	}

	// 退出时等待存储收尾，确保关闭错误在释放资源前被记录。
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

// 解包flv数据
func (s *Stream) unpack(p *packet.Packet) bool {
	s.cache.isKeyframe = false
	media, err := packet.ClassifyMedia(p)
	if err != nil {
		log.Errorf("%s,key='%s'", err, s.Info().Key)
		return false
	}
	switch media.Kind {
	case packet.MediaIgnored:
		return false
	case packet.MediaVideo:
		s.cache.isKeyframe = media.Keyframe
	case packet.MediaAVCHeader:
		repeated := s.videoHeaderSeen
		s.videoHeaderSeen = true
		if err := s.mixer.SaveAVCHeader(p); err != nil {
			log.Error(err)
		}
		return repeated && s.store.IsOpened()
	case packet.MediaAACHeader:
		repeated := s.audioHeaderSeen
		s.audioHeaderSeen = true
		if err := s.mixer.SaveAACHeader(p); err != nil {
			log.Error(err)
		}
		return repeated && s.store.IsOpened()
	}
	return true
}

// 打开文件并且初始化FLV头
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

	s.flv.Reset()

	err := s.store.Open()
	if err != nil {
		if !s.store.WaitingFinalization() {
			log.Error(err)
		}
		return false
	}

	cfg := &config.Get().Metadata
	obj := amf.Object{
		"Provider": cfg.Provider,
		"Service":  cfg.Service,
		"Created":  s.store.CreatedTime(),
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
	return "flv"
}
