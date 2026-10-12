package rtmp

import (
	"context"
	"errors"
	"hash/fnv"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/rtmp/core"
	"sync/atomic"
)

// Conn 管理RTMP流
type Conn struct {
	ctx        context.Context
	cancel     context.CancelFunc
	checkin    atomic.Int64
	stream     sync.Map // 本站
	source     sync.Map // 源站
	locks      [64]sync.Mutex
	closed     atomic.Bool
	closeOnce  sync.Once
	serverMu   sync.Mutex
	servers    []*Server
	background sync.WaitGroup
	origins    sync.Map
	players    []EmbedWriter
	pull       func(context.Context, *base.Info, string, time.Duration) (base.ReadCloser, error)
}

func (conn *Conn) keyLock(key string) *sync.Mutex {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return &conn.locks[hash.Sum32()%uint32(len(conn.locks))]
}

// NewConn 管理RTMP流
func NewConn(ctx context.Context) *Conn {
	ctx, cancel := context.WithCancel(ctx)
	ret := &Conn{
		ctx: ctx, cancel: cancel, pull: pullOrigin,
	}

	ret.background.Add(2)
	go func() { defer ret.background.Done(); ret.cleanStream() }()
	go func() { defer ret.background.Done(); ret.cleanSource() }()

	return ret
}

func (connection *Conn) PublicationKeys() []string {
	var keys []string
	connection.stream.Range(func(key, value any) bool {
		stream := value.(*Stream)
		if !stream.info.IsExternal && stream.Reader() != nil {
			keys = append(keys, key.(string))
		}
		return true
	})
	return keys
}

// StreamKeys 流ID列表
func (c *Conn) StreamKeys() []string {
	var keys []string

	c.stream.Range(func(key, _ interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})

	return keys
}

// Reader 获取流列表
func (c *Conn) Reader(key string) *core.Reader {
	value, ok := c.stream.Load(key)
	if !ok {
		return nil
	}

	s := value.(*Stream)

	sr := s.Reader()
	if sr == nil {
		return nil
	}

	sc := sr.ReadCloser
	if sc == nil {
		panic("ReadCloser is nil")
	}

	switch sc.(type) {
	case *core.Reader:
		return sc.(*core.Reader)
	}

	return nil
}

// Writer 获取订阅列表
func (c *Conn) Writer(key string) []*core.Funnel {
	value, ok := c.stream.Load(key)
	if !ok {
		return nil
	}

	s := value.(*Stream)
	writer := s.Writer()

	var ret []*core.Funnel
	for _, sw := range writer {
		wc := sw.WriteCloser

		switch wc.(type) {
		case *core.Funnel:
			w := wc.(*core.Funnel)
			ret = append(ret, w)
		}
	}
	return ret
}

// WriterActive writer活跃统计
func (c *Conn) WriterActive(key string) (int64, int64) {
	value, ok := c.stream.Load(key)
	if !ok {
		return 0, 0
	}

	s := value.(*Stream)
	return s.client.count.Load(), s.client.checkIn.Load()
}

// 定期淘汰源站
func (c *Conn) cleanSource() {
	for {
		finder := config.GetFinder()
		expire := finder.Expire * time.Second

		select {
		case <-c.ctx.Done():
			return
		case <-time.After(expire):
		}

		now := time.Now().Unix()
		c.source.Range(func(key, value interface{}) bool {
			item := value.(*sourceCache)

			// 定期淘汰源站
			if now-item.time > int64(finder.Expire) {
				c.source.CompareAndDelete(key, item)
			}

			return true
		})
	}
}

// 如果stream没有活跃的读写连接, 则移除stream
func (c *Conn) cleanStream() {
	interval := time.Duration(config.Get().RTMP.EdgeCheck)
	ticker := time.NewTicker(interval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}

		c.stream.Range(func(key, value interface{}) bool {
			lock := c.keyLock(key.(string))
			lock.Lock()
			defer lock.Unlock()
			if actual, ok := c.stream.Load(key); !ok || actual != value {
				return true
			}
			s := value.(*Stream)

			// 服务退出的清理
			if s.inside.control.IsDone() {
				c.stream.CompareAndDelete(key, s)
				return true
			}

			if s.health() {
				return true
			}

			err := s.terminate(true)
			if err != nil {
				log.Error(err)
			}

			c.stream.CompareAndDelete(key, s)
			return true
		})
	}
}

// 填充源站列表，有源站可用则返回true，否则返回false
func (c *Conn) find(ds *sourceCache, info *base.Info) bool {
	return c.findWithConfig(ds, info, config.Get())
}

func discoveredRTMPURL(app, instance, advertisement string) (string, error) {
	endpoint, err := config.DiscoveryEndpoint(advertisement, "rtmp")
	if err != nil {
		return "", err
	}
	if endpoint == "" {
		return "", errors.New("empty RTMP discovery endpoint")
	}
	return (&url.URL{Scheme: "rtmp", Host: endpoint, Path: "/" + app + "/" + instance}).String(), nil
}

func (c *Conn) findWithConfig(ds *sourceCache, info *base.Info, cfg *config.Config) bool {
	app := cfg.GetApp(info.App)
	if app == nil {
		log.Warnf("app is nil,app name='%s'", info.App)
		return false
	}

	// 服务发现得到host，组成rtmp地址。初始状态下的源站会被标记为不可用
	advice, err := cfg.Finder.FindAdvice(info.App, info.Instance)
	if err != nil {
		log.Errorf("[%s]Find advice failed.Err=%s", info.Key, err)
	}
	if advice != "" {
		endpoint, err := discoveredRTMPURL(info.App, info.Instance, advice)
		if err != nil {
			log.Errorf("[%s]invalid RTMP discovery endpoint: %v", info.Key, err)
		} else {
			ss := &sourceStatus{
				usable:   false,
				deadTime: 0,
				pullHost: endpoint,
			}
			ds.data = append(ds.data, ss)
			log.Infof("[%s]RTMP found target data source: %s", info.Key, endpoint)
		}
	}

	// 通过配置得到host，组成rtmp地址。
	staticPullList := app.StaticPullList(info.Instance)
	for _, v := range staticPullList {
		ss := &sourceStatus{
			usable:   false,
			deadTime: 0,
			pullHost: v,
		}

		ds.data = append(ds.data, ss)
	}

	if len(ds.data) == 0 {
		return false
	}

	return true
}

// InstallWriter 数据源将被写入writer。
func (connection *Conn) InstallWriter(writer base.WriteCloser) {
	connection.installOriginWriter(writer)
}

// InstallReader 从reader中取出数据源
func (c *Conn) InstallReader(r base.ReadCloser) {
	lock := c.keyLock(r.Info().Key)
	lock.Lock()
	defer lock.Unlock()
	if c.closed.Load() {
		r.Close()
		return
	}
	cfg := config.Get()
	app := cfg.GetApp(r.Info().App)
	if app == nil {
		r.Close()
		return
	}
	value, ok := c.stream.Load(r.Info().Key)
	if !ok {
		ns := newStream(r.Info(), cfg, app)

		err := ns.addReader(&StreamReader{r})
		if err != nil {
			log.Error(err)

			// terminate new stream server(include all writers)
			err = ns.terminate(true)
			if err != nil {
				log.Error(err)
			}
		}

		c.stream.Store(r.Info().Key, ns)
		return
	}

	s := value.(*Stream)

	ns := newStream(r.Info(), cfg, app)

	// terminate old stream server(without writers)
	err := s.terminate(false)
	if err != nil {
		log.Trace(err)
	}
	if s.HasReader() {
		<-s.dispatchDone
	}
	s.migrateTo(ns)

	err = ns.addReader(&StreamReader{r})
	if err != nil {
		log.Error(err)

		// terminate new stream server(with writers)
		err = ns.terminate(true)
		if err != nil {
			log.Error(err)
		}
		return
	}

	// 在上面进行流服务关闭操作，需要重新插入任务
	c.stream.Store(r.Info().Key, ns)

	log.Infof("[%s]Target stream was republished successfully", r.Info().Key)
}

// Checkin 最近一个writer的注册时间
func (c *Conn) Checkin() int64 {
	return c.checkin.Load()
}

// Close 退出清理工作
func (c *Conn) Close() {
	c.closeOnce.Do(c.close)
}

func (c *Conn) close() {
	c.serverMu.Lock()
	c.closed.Store(true)
	servers := c.servers
	c.servers = nil
	c.serverMu.Unlock()
	if c.cancel != nil {
		c.cancel()
	}
	for _, server := range servers {
		server.Close()
	}
	for index := range c.locks {
		c.locks[index].Lock()
		c.locks[index].Unlock()
	}
	c.origins.Range(func(_, value any) bool {
		value.(*originFlight).cancel()
		return true
	})
	c.origins.Range(func(_, value any) bool {
		<-value.(*originFlight).done
		return true
	})
	wg := sync.WaitGroup{}

	c.stream.Range(func(key, value interface{}) bool {
		s := value.(*Stream)

		// terminate inactive publisher
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock := c.keyLock(key.(string))
			lock.Lock()
			defer lock.Unlock()

			err := s.terminate(true)
			if err != nil {
				log.Tracef("[%s]err:%s", s.info.Key, err)
			}
			if s.HasReader() {
				<-s.dispatchDone
			}
		}()

		c.stream.CompareAndDelete(key, s)
		return true
	})

	wg.Wait()
	c.background.Wait()
}

// StartRTMP 启动RTMP服务
func (c *Conn) StartRTMP(w ...EmbedWriter) (net.Listener, error) {
	c.serverMu.Lock()
	defer c.serverMu.Unlock()
	if c.closed.Load() {
		return nil, net.ErrClosed
	}
	cfg := config.Get().RTMP

	l, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	log.Info("Rtmp [tcp] Listening on ", cfg.Addr)

	rtmpSrv := NewRtmpServer(c, w...)
	c.servers = append(c.servers, rtmpSrv)
	c.players = append([]EmbedWriter(nil), w...)
	c.background.Add(1)
	go func() { defer c.background.Done(); rtmpSrv.Serve(l) }()
	return l, nil
}
