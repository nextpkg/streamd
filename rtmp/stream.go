// Package rtmp 管理RTMP读写连接
package rtmp

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/control"
	"github.com/nextpkg/streamd/gop"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/metrics"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/rtmp/core"

	errors "github.com/nextpkg/streamd/internal/errutil"
	"sync/atomic"
)

// Stream RTMP流
type Stream struct {
	sync.RWMutex
	info         *base.Info    // 流服务的描述
	rConn        *StreamReader // reader connection，读连接
	wConn        sync.Map      // writer connection，写连接
	active       active        // 活跃统计
	inside       inside        // 流程管理
	client       client        // 客户统计
	app          *config.App
	dispatchDone chan struct{}
	relayDone    chan struct{}
}

// 流程管理
type inside struct {
	event   *event           // 发布过程的事件
	cache   *gop.Cache       // 关键帧缓存
	control *control.Control // 流程控制器
}

// 活跃统计
type active struct {
	count atomic.Int64 // player总活跃数
	time  int64        // 最后活跃时间, 用来控制连接超时断开
}

// 客户统计
type client struct {
	count   atomic.Int64
	checkIn atomic.Int64
}

// NewStream 每个stream都使用一个新的UUID
func NewStream(info *base.Info) *Stream {
	cfg := config.Get()
	return newStream(info, cfg, cfg.GetApp(info.App))
}

func newStream(info *base.Info, cfg *config.Config, app *config.App) *Stream {

	// 用户事件
	event := &event{
		onPublish:     fireEvent("OnPublish", app.Event.OnPublish, info.Key),
		onPublishDone: fireEvent("OnPublishDone", app.Event.OnPublishDone, info.Key),
	}

	// 流管理实体
	ret := &Stream{
		info:         info,
		app:          app,
		dispatchDone: make(chan struct{}),
		relayDone:    make(chan struct{}),
		active: active{
			time: time.Now().Unix(),
		},
		inside: inside{
			event:   event,
			cache:   gop.NewCache(cfg.RTMP.Gop),
			control: control.NewControl(false),
		},
	}

	return ret
}

// Reader RTMP流读进连接
func (s *Stream) Reader() *StreamReader {
	s.RLock()
	defer s.RUnlock()
	if s.inside.control.IsDone() {
		return nil
	}
	return s.rConn
}

// Writer RTMP流写出连接
func (s *Stream) Writer() []*StreamWriter {
	var ret []*StreamWriter

	s.wConn.Range(func(key, value interface{}) bool {
		w := value.(*StreamWriter)

		ret = append(ret, w)
		return true
	})

	return ret
}

// HasReader 判断是否已经有Reader
func (s *Stream) HasReader() bool {
	s.RLock()
	defer s.RUnlock()

	return s.rConn != nil
}

// 启动收集/派发流程（由上层业务决定是否加入stream）
func (s *Stream) addReader(r *StreamReader) error {
	// 登记生产者
	s.Lock()
	if s.inside.control.IsDone() || s.rConn != nil {
		s.Unlock()
		return errors.New("stream server is cancel or reader is initialized")
	}

	s.rConn = r
	s.Unlock()

	// 服务注册
	s.register()

	// 启动分发流程
	go s.start()

	// 启动静态转播
	go s.staticRelay()

	// 启动消费者活跃数统计
	go s.restate()

	// 触发回调
	s.inside.event.onPublish()

	return nil
}

// 向conn里添加 {uid: writer}, 使用UID作为用户标识
func (s *Stream) addWriter(w *StreamWriter) error {
	s.Lock()
	defer s.Unlock()
	if s.inside.control.IsDone() {
		return core.ErrWriterWasCanceled
	}
	_, loaded := s.wConn.LoadOrStore(w.Info().UID, w)
	if loaded {
		return fmt.Errorf("[%s]duplicate writer", s.info.Key)
	}

	if w.Info().IsExternal {
		s.client.checkIn.Store(time.Now().Unix())
		s.client.count.Add(1)
	}
	return nil
}

func (s *Stream) delWriter(w *StreamWriter, key interface{}) {
	s.removeWriter(w, key, false)
}

func (s *Stream) removeWriter(w *StreamWriter, key interface{}, abort bool) {
	if !s.wConn.CompareAndDelete(key, w) {
		return
	}
	if terminal, ok := w.WriteCloser.(interface{ Abort() }); abort && ok {
		terminal.Abort()
	} else {
		w.Close()
	}
	if w.Info().IsExternal {
		s.client.count.Add(-1)
	}
}

// 服务注册
func (s *Stream) register() {
	metrics.AddGlobal(metrics.Channels, 1)

	// 只有属于内部服务的才会被记录下来, 外部服务(指通过回源方式)不会被上报
	if s.info.IsExternal {
		return
	}

	go func() {
		for {
			expire := config.GetFinder().Interval * time.Second

			select {
			case <-s.inside.control.Done():
				return
			case <-time.After(expire):
				finder := config.GetFinder()
				advice, err := finder.FindAdvice(s.info.App, s.info.Instance)
				if err != nil {
					if finder != config.GetFinder() {
						continue
					}
					select {
					case <-s.inside.control.Done():
						return
					default:
					}
					log.Errorf("[%s]Find Advice failed.Err=%s", s.info.Key, err)
					continue
				}

				// IP被正确注册了，且正确续期
				extend := finder.Keepalive(s.info.App, s.info.Instance)
				if advice == finder.GetAdvice() && extend {
					continue
				}
				if advice != "" {
					log.Infof("[%s]Source IP is changed", s.info.Key)
				}

				finder.Register(s.info.App, s.info.Instance)
			}
		}
	}()

	config.GetFinder().Register(s.info.App, s.info.Instance)
}

// 服务注销
func (s *Stream) deregister() {
	metrics.AddGlobal(metrics.Channels, -1)

	// 只有属于内部服务的才会被记录下来, 外部服务(指通过回源方式)不会被上报，因此也不需要被注销
	if s.info.IsExternal {
		return
	}

	config.GetFinder().Deregister(s.info.App, s.info.Instance)
}

// 静态转播，使用动态转播模拟实现
func (s *Stream) staticRelay() {
	defer close(s.relayDone)
	cfg := config.Get()
	app := s.app

	pullHost := app.StaticRtmp(cfg.RTMP.Addr, s.info.Instance)
	pushList := app.StaticPushList(s.info.Instance)

	if len(pushList) == 0 || s.inside.control.IsDone() {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var workers sync.WaitGroup
	for _, pushHost := range pushList {
		relay := core.NewDynamicRelay(pullHost, pushHost, time.Duration(cfg.RTMP.DialTimeout)*time.Second)
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { relay.Stop(); <-relay.Wait() }()
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for ctx.Err() == nil {
				if err := relay.StartContext(ctx); err != nil && ctx.Err() == nil {
					log.Error(err)
					relay.RenewConnection()
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	<-s.inside.control.Done()
	cancel()
	workers.Wait()
}

// 终止收集/派发流程，指定closeWriter来标明是否需要同时关闭writer
func (s *Stream) terminate(closeWriter bool) error {
	s.Lock()
	if s.inside.control.IsDone() {
		s.Unlock()
		return nil
	}

	s.inside.control.Cancel()
	reader := s.rConn
	s.Unlock()

	// 注销
	if reader != nil {
		s.deregister()
	}

	// 关闭生产者
	if reader != nil {
		reader.Close()
	}

	// 关闭消费者
	if closeWriter {
		s.wConn.Range(func(key, value interface{}) bool {
			w := value.(*StreamWriter)
			s.delWriter(w, key)
			return true
		})
	}

	// 触发回调
	if reader != nil {
		s.inside.event.onPublishDone()
	}

	log.Infof("[%s]Client terminated", s.info.Key)
	return nil
}

// 启动收集/派发流程（在读不到数据的时候做关闭操作）
func (s *Stream) start() {
	defer func() { <-s.relayDone; close(s.dispatchDone) }()
	for {
		if s.inside.control.IsDone() {
			return
		}

		// 读取数据包
		p := s.readPacket()
		if p == nil || s.inside.control.IsDone() {
			return
		}

		s.dispatch(p)
	}
}

func (s *Stream) readPacket() *packet.Packet {
	p := &packet.Packet{}

	t1 := time.Now().UnixNano()

	err := s.rConn.Read(p)

	t2 := time.Now().UnixNano() - t1
	metrics.Average(s.info.App, s.info.Instance, metrics.ReadCost, float64(t2/1000))

	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.Errorf("[%s]Read error: %s", s.info.Key, err)
		}

		err = s.terminate(true)
		if err != nil {
			log.Debugf("[%s]Terminate error: %s", s.info.Key, err)
		}
		return nil
	}

	return p
}

// 向所有消费者发送数据
// 潜在风险: 可能的性能瓶颈，有需要可实现二级转发，提高转发效率
func (s *Stream) dispatch(p *packet.Packet) {
	t1 := time.Now().UnixNano()

	// 缓存数据包
	err := s.writeCache(p)
	if err != nil {
		log.Errorf("[%s]Write to cache failed: %s", s.info.Key, err)
	}

	// 向所有消费者发送数据
	s.wConn.Range(func(key, value interface{}) bool {
		w := value.(*StreamWriter)

		// 转发数据, 如果未初始化, 那么发送GOP包, 如果已经初始化, 那么发送普通数据包
		if w.hasGOP {
			err := w.Write(p)
			if err != nil {
				info := w.Info()

				const msg = "[%s]external stream: %t, error: %s"
				var arg = []interface{}{info.Key, info.IsExternal, err}

				switch err {
				case core.ErrWriterWasCanceled:
					s.removeWriter(w, key, true)
					log.Debugf(msg, arg...)
				case core.ErrQueueSaturated:
					if info.IsExternal {
						s.removeWriter(w, key, true)
					}
					fallthrough
				default:
					log.Errorf(msg, arg...)
				}
			}

			return true
		}

		// 【首屏秒开功能】-- 先发送GOP包给客户端
		err := s.inside.cache.SendTo(w)
		if err != nil && err != core.ErrWriterWasCanceled {
			log.Errorf("[%s]SendTo failed:%s", s.info.Key, err)
		}
		if err != nil && w.Info().IsExternal {
			s.removeWriter(w, key, true)
			return true
		}

		// 客户端已经拥有了GOP数据
		w.hasGOP = true
		return true
	})

	t2 := time.Now().UnixNano() - t1
	metrics.Average(s.info.App, s.info.Instance, metrics.WriteCost, float64(t2/1000))
}

// 迁移消费者到目标流服务中
func (s *Stream) migrateTo(dst *Stream) {
	s.wConn.Range(func(key, value interface{}) bool {
		w := value.(*StreamWriter)
		if !s.wConn.CompareAndDelete(key, w) {
			return true
		}
		if w.Info().IsExternal {
			s.client.count.Add(-1)
		}

		// 1. 新生产者流的PTS从0开始，我们需要记录消费者的PTS，使消费者的PTS在迁移前后是连续的
		w.RebaseTime()
		w.hasGOP = false

		// 2. 将已有的消费者插入新的流服务中
		err := dst.addWriter(w)
		if err != nil {
			w.Close()
			log.Error(err)
		}
		return true
	})
}

// 在生产者活跃的条件下，更新消费者的活跃数并退出不活跃的消费者
func (s *Stream) restate() {
	second := time.Duration(config.Get().RTMP.EdgeCheck)
	ticker := time.NewTicker(second * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.inside.control.Done():
			return
		case <-ticker.C:
		}
		s.RLock()
		if s.inside.control.IsDone() {
			s.RUnlock()
			return
		}

		if s.rConn != nil && s.rConn.IsTimeout() {
			log.Infof("[%s]Source connection is timeout", s.rConn.Info().Key)
			s.RUnlock()
			continue
		}

		n := int64(0)
		s.wConn.Range(func(key, value interface{}) bool {
			w := value.(*StreamWriter)

			// 只统计外部请求（把HLS、录制器等内嵌客户端排除在外)
			if !w.Info().IsExternal {
				return true
			}

			if w.IsTimeout() {
				log.Errorf("[%s]Writer is timeout", w.Info().Key)
				s.removeWriter(w, key, true)
				return true
			}

			n++
			return true
		})
		s.active.count.Store(n)
		s.RUnlock()
	}
}

func (s *Stream) health() bool {
	s.RLock()
	conn := s.rConn
	s.RUnlock()

	observed := time.Now()
	now := observed.Unix()
	overtime := config.Get().RTMP.EdgeAlive
	if conn == nil {
		return now-s.active.time <= overtime
	}

	if conn.IsTimeout() {
		return false
	}

	if !s.info.IsExternal && !s.app.Passerby {
		s.active.time = now
		return true
	}

	active := s.active.count.Load()
	if active >= 1 {
		s.active.time = now
		return true
	}
	viewing := false
	s.wConn.Range(func(_, value any) bool {
		writer := value.(*StreamWriter)
		if !writer.Info().IsExternal {
			if viewer, ok := writer.WriteCloser.(interface{ ViewerDeadline() time.Time }); ok && viewer.ViewerDeadline().After(observed) {
				viewing = true
				return false
			}
		}
		return true
	})
	if viewing {
		return true
	}

	// 转播、回源的数据源, 超时后才会退出服务
	if now-s.active.time <= overtime {
		return true
	}

	log.Infof("[%s]Data source idling over %ds.", s.info.Key, overtime)

	return false
}
