package hfs

import (
	"net/http"
	"sync"
	"time"

	"github.com/nextpkg/goav/amf"
	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/container/flv"
	errors "github.com/nextpkg/streamd/internal/errutil"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/rtmp/core"
)

// Stream HFS数据转换
type Stream struct {
	*core.Universal

	once            sync.Once
	started         bool
	videoHeaderSeen bool
	audioHeaderSeen bool
	response        http.ResponseWriter
	mixer           *flv.Mixer // flv混合器
	stateMu         sync.Mutex
	writing         bool
	aborted         bool
	finished        bool
}

// NewStream HFS数据转换 IsExternal: true
func NewStream(app, instance string, w http.ResponseWriter) *core.Funnel {
	info := base.NewInfo(app, instance, true)

	s := &Stream{
		Universal: core.NewUniversal(info),
		mixer:     flv.NewMixer(w),
		response:  w,
	}

	cfg := config.Get().Metadata

	obj := amf.Object{
		"Provider": cfg.Provider,
		"Service":  cfg.Service,
	}

	err := s.mixer.SaveMetadata(obj)
	if err != nil {
		panic(errors.Wrap(err, "save metadata failed"))
	}

	return core.NewFunnel(s)
}

// Write 读取数据包，转换成hfs格式后输出
func (s *Stream) Write(p *packet.Packet) error {
	s.stateMu.Lock()
	if s.aborted {
		s.stateMu.Unlock()
		return core.ErrWriterWasCanceled
	}
	s.writing = true
	s.stateMu.Unlock()
	defer func() {
		s.stateMu.Lock()
		s.writing = false
		s.stateMu.Unlock()
	}()
	if !s.unpack(p) {
		return nil
	}

	var err error

	s.once.Do(func() {
		err = s.mixer.SetFlvHeader()
		s.started = err == nil
		if err != nil {
			log.Error(err)
		}
	})

	if err != nil {
		s.once = sync.Once{}
	}

	err = s.mixer.Mux(p, p.Baseline)
	if err != nil {
		return errors.Wrapf(err, "mux flv failed,key='%s'", s.Info().Key)
	}
	if flusher, ok := s.response.(http.Flusher); ok {
		flusher.Flush()
	}

	return nil
}

func (s *Stream) Abort() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.finished || s.aborted {
		return
	}
	s.aborted = true
	if s.writing {
		_ = http.NewResponseController(s.response).SetWriteDeadline(time.Now())
	}
}

func (s *Stream) After() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.finished = true
}

// 分离FLV和数据
func (s *Stream) unpack(p *packet.Packet) bool {
	header := p.Header

	switch p.Type {
	case packet.PktVideo:
		vh := header.(packet.VideoPacketHeader)

		// 暂只支持"H264"
		if !vh.IsCodecAvc() {
			log.Errorf("support H264 only,key='%s',code id=%d", s.Info().Key, vh.CodecID())
			return false
		}

		if vh.IsSeqHdr() {
			repeated := s.videoHeaderSeen
			s.videoHeaderSeen = true
			err := s.mixer.SaveAVCHeader(p)
			if err != nil {
				log.Error(err)
			}

			return repeated && s.started
		}

		if vh.IsEndOfSeq() {
			return false
		}
	case packet.PktAudio:
		ah := header.(packet.AudioPacketHeader)

		// 只支持"aac"
		if !ah.IsSoundAAC() {
			log.Errorf("support AAC only,key='%s',sound format=%d",
				s.Info().Key, ah.SoundFormat())
			return false
		}

		if ah.IsAACSeqHdr() {
			repeated := s.audioHeaderSeen
			s.audioHeaderSeen = true
			err := s.mixer.SaveAACHeader(p)
			if err != nil {
				log.Error(err)
			}

			return repeated && s.started
		}
	case packet.PktMetadata:
		// 忽略输入元数据；输出元数据由当前服务配置生成。
		return false
	}

	return true
}

// Name writer's name
func (s *Stream) Name() string {
	return "http-flv"
}
