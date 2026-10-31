// Package hls 接收flv数据, 复用成ts数据
package hls

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nextpkg/goav/amf"
	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/container/ts"
	"github.com/nextpkg/streamd/hls/item"
	errors "github.com/nextpkg/streamd/internal/errutil"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/rtmp/core"
)

// cache HLS元数据
type cache struct {
	isKeyframe  bool
	composition uint32 // pts和dts相差的时间增量
}

// Stream HLS后端流提供服务
type Stream struct {
	*core.Universal
	ts           *bytes.Buffer
	cache        *cache
	m3u8         *item.TsItemCache
	mixer        *ts.Mixer
	fragment     *base.Fragment
	param        *config.AppHLS
	stop         chan struct{}
	stopOnce     sync.Once
	active       atomic.Bool
	viewerUntil  atomic.Int64
	viewerMu     sync.Mutex
	signatures   *config.SignatureSet
	seenRevision uint64
}

// NewStream HLS后端流提供服务
func NewStream(info *base.Info) *core.Funnel {
	snapshot := config.Get()
	app := snapshot.GetApp(info.App)
	if app == nil {
		panic("HLS application is not configured")
	}
	settings := app.HLS
	if settings.PlaylistLength <= 0 {
		settings.PlaylistLength = 2
	}
	param := &settings

	cache := &cache{
		isKeyframe:  false,
		composition: 0,
	}

	m3u8 := item.NewTsItemCache(param.PlaylistLength)

	// ts 混合器
	buffer := bytes.NewBuffer(nil)
	mixer := ts.NewMixer(buffer)

	ret := &Stream{
		Universal:    core.NewUniversal(info),
		stop:         make(chan struct{}),
		ts:           buffer,
		cache:        cache,
		m3u8:         m3u8,
		mixer:        mixer,
		fragment:     base.NewFragment(param.Fragment),
		param:        param,
		signatures:   config.NewSignatureSet(param.SecureKey, param.SecureTTL),
		seenRevision: snapshot.Revision,
	}
	ret.setActive(param.Enable)
	config.StartApplication("hls", info.Key, info.UID, snapshot.Revision, "waiting_keyframe")

	cfg := config.Get().Metadata

	obj := amf.Object{
		"Provider": cfg.Provider,
		"Service":  cfg.Service,
	}

	err := ret.mixer.SaveMetadata(obj)
	if err != nil {
		panic(errors.Wrap(err, "save metadata failed"))
	}

	return core.NewFunnel(ret)
}

// Write 处理来自数据源的数据，内嵌客户端遇到错误无需退出
func (s *Stream) Write(p *packet.Packet) error {
	snapshot := config.Get()
	if snapshot.Revision != s.seenRevision {
		s.seenRevision = snapshot.Revision
		config.ReportApplication("hls", s.Info().Key, s.Info().UID, snapshot.Revision, "waiting_keyframe")
	}
	if !s.separate(p) {
		return nil
	}
	if packet.IsVideoBoundary(p) {
		s.applyConfiguration(snapshot)
	}
	if !s.active.Load() {
		return nil
	}

	if !s.split(p.Baseline) {
		return nil
	}

	if !s.update(p) {
		return nil
	}

	err := s.mixer.Mux(p)
	if err != nil {
		log.Errorf("err=%s,key=%s", err, s.Info().Key)
		return nil
	}

	return nil
}

// After 流结束后的处理过程
func (s *Stream) After() {
	defer config.ForgetApplication("hls", s.Info().Key, s.Info().UID)
	s.setActive(false)
	s.flush()

	// 总m3u8长度，等待最后一个用户播放完
	s.m3u8.MarkEnd()
	timer := time.NewTimer(s.m3u8.Duration())
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-s.stop:
	}

	s.m3u8.Cleanup()
}

func (s *Stream) applyConfiguration(snapshot *config.Config) {
	app := snapshot.GetApp(s.Info().App)
	if app == nil {
		config.ReportApplication("hls", s.Info().Key, s.Info().UID, snapshot.Revision, "retained_for_existing_stream")
		return
	}
	next := app.HLS
	if next.PlaylistLength <= 0 {
		next.PlaylistLength = s.param.PlaylistLength
	}
	if next != *s.param {
		wasActive := s.active.Load()
		if wasActive {
			s.flush()
		}
		s.fragment.Create()
		s.param = &next
		s.signatures.Update(next.SecureKey, next.SecureTTL)
		s.m3u8.Resize(next.PlaylistLength)
		s.setActive(next.Enable)
		if next.Enable {
			s.m3u8.MarkLive(!wasActive)
			if err := s.mixer.SetTsHeader(); err != nil {
				log.Error("HLS configuration boundary: ", err)
			}
		} else {
			s.m3u8.MarkEnd()
		}
	}
	state := "active"
	if !next.Enable {
		state = "disabled"
	}
	config.ReportApplication("hls", s.Info().Key, s.Info().UID, snapshot.Revision, state)
}

func (s *Stream) Available() bool { return s.active.Load() || s.m3u8.HasItems() }

func (s *Stream) setActive(enabled bool) {
	s.viewerMu.Lock()
	defer s.viewerMu.Unlock()
	s.active.Store(enabled)
	if !enabled {
		s.viewerUntil.Store(0)
	}
}

func (s *Stream) refreshViewer(now time.Time, window time.Duration) {
	s.viewerMu.Lock()
	defer s.viewerMu.Unlock()
	select {
	case <-s.stop:
		return
	default:
	}
	if !s.active.Load() || !s.m3u8.IsLive() {
		return
	}
	window = max(time.Duration(config.Get().RTMP.EdgeAlive)*time.Second, window)
	deadline := now.Add(window).UnixNano()
	if deadline > s.viewerUntil.Load() {
		s.viewerUntil.Store(deadline)
	}
}

func (s *Stream) ViewerDeadline() time.Time {
	select {
	case <-s.stop:
		return time.Time{}
	default:
	}
	if !s.active.Load() || !s.m3u8.IsLive() {
		return time.Time{}
	}
	deadline := s.viewerUntil.Load()
	if deadline == 0 {
		return time.Time{}
	}
	return time.Unix(0, deadline)
}

func (s *Stream) CheckSignature(token, expiration string) int {
	return s.signatures.Check(s.Info().Key, token, expiration)
}

func (s *Stream) Discontinuity() {
	if !s.active.Load() {
		return
	}
	s.flush()
	s.fragment.Create()
	s.m3u8.MarkLive(true)
	if err := s.mixer.SetTsHeader(); err != nil {
		log.Error("HLS source transition: ", err)
	}
}

// Close 流关闭操作
func (s *Stream) Close() {
	s.viewerMu.Lock()
	defer s.viewerMu.Unlock()
	s.viewerUntil.Store(0)
	s.stopOnce.Do(func() { close(s.stop) })
}

// 分离FLV
func (s *Stream) separate(p *packet.Packet) bool {
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
		s.cache.composition = uint32(media.CompositionTime)
	case packet.MediaAVCHeader:
		if err := s.mixer.SaveAVCHeader(p); err != nil {
			log.Error(err)
		}
		return false
	case packet.MediaAACHeader:
		if err := s.mixer.SaveAACHeader(p); err != nil {
			log.Error(err)
		}
		return false
	}
	return true
}

// 切割TS
func (s *Stream) split(timestamp uint32) bool {
	duration := s.fragment.DurationAt(timestamp)
	if (s.cache.isKeyframe || s.param.Fragment == 0) && duration >= s.param.Fragment*1000 {
		if duration > s.fragment.Duration() {
			s.fragment.Update(timestamp)
		}
		s.flush()

		s.fragment.Create()

		err := s.mixer.SetTsHeader()
		if err != nil {
			log.Errorf("err=%s,key=%s", err, s.Info().Key)
		}
	}

	return true
}

// 更新PTS
func (s *Stream) update(p *packet.Packet) bool {
	s.fragment.Update(p.Baseline)

	// update pts
	err := s.mixer.Update(p, p.Baseline, s.cache.composition)
	if err != nil {
		log.Errorf("err=%s,key=%s", err, s.Info().Key)
		return false
	}

	return true
}

func (s *Stream) flush() {
	if s.ts.Len() <= 0 {
		return
	}

	duration := s.fragment.Duration()
	sequence := s.fragment.Sequence()

	optional := s.param.AntiLeechQuery(s.Info().Key)
	if len(optional) > 0 {
		optional = "?" + optional
	}

	itemName := s.Info().Instance + "/" + sequence + ".ts" + optional

	tsItem := item.NewTsItem(itemName, duration, s.fragment.Index(), s.ts.Bytes())
	s.m3u8.SetItem(sequence, tsItem)

	s.ts.Reset()
}

// Name writer's name
func (s *Stream) Name() string {
	return "hls"
}
