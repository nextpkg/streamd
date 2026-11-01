package console

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/errutil"
	"github.com/nextpkg/streamd/internal/httpx"
	"github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/rtmp"
	"github.com/nextpkg/streamd/rtmp/core"
)

// Server 控制台
type Server struct {
	Handler      rtmp.Handler
	Session      sync.Map
	relayMu      sync.Mutex
	relayGate    chan struct{}
	resolveIP    func(context.Context, string) ([]net.IPAddr, error)
	closed       bool
	queryMu      sync.Mutex
	queryContext context.Context
	queryCancel  context.CancelFunc
	queryClosed  bool
	queryWait    sync.WaitGroup
}

// NewServer 控制台
func NewServer(h rtmp.Handler) *Server {
	return &Server{
		Handler: h,
	}
}

// Init 注册
func (s *Server) Init(r *http.ServeMux) *http.ServeMux {

	register := func(method, path string, handler http.HandlerFunc) {
		guarded := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != method && (method != http.MethodGet || request.Method != http.MethodHead) {
				writer.Header().Set("Allow", method)
				httpx.NewWriter(writer).QuickSendJSON(http.StatusMethodNotAllowed, "method not allowed", nil)
				return
			}
			handler(writer, request)
		})
		guarded = s.managementRequest(guarded)
		r.HandleFunc(path, guarded)
	}
	register(http.MethodGet, "/admin/stat/keys", s.statKeys)
	register(http.MethodGet, "/admin/stat/pub/info", s.statPubInfo)
	register(http.MethodGet, "/admin/stat/sub/info", s.statSubInfo)
	register(http.MethodGet, "/admin/stat/sub/online", s.statSubOnline)
	register(http.MethodPost, "/admin/relay/{action}", s.relay)
	register(http.MethodGet, "/admin/config", s.config)

	return r
}

// 测试RTMP的URL是否合规
func (s *Server) checkRtmpURL(rtmp *string) error {
	ctx, cancel := context.WithTimeout(context.Background(), queryIOBudget)
	defer cancel()
	return s.checkRtmpURLContext(ctx, rtmp)
}

func (s *Server) checkRtmpURLContext(ctx context.Context, rtmp *string) error {
	if rtmp == nil {
		return errutil.New("nil rtmp")
	}

	if len(*rtmp) == 0 {
		return errutil.New("empty rtmp url")
	}

	location := *rtmp
	if !strings.Contains(location, "://") {
		location = "rtmp://" + strings.TrimPrefix(location, "//")
	}
	u, err := url.Parse(location)
	if err != nil {
		return errutil.New("invalid rtmp url")
	}

	// scheme
	if len(u.Scheme) == 0 {
		u.Scheme = "rtmp"
	}
	if u.Scheme != "rtmp" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return errutil.New("invalid url protocol")
	}

	// path
	path := strings.TrimLeft(u.Path, "/")
	ps := strings.Split(path, "/")
	if len(ps) != 2 || ps[0] == "" || ps[1] == "" {
		return errutil.New("missing rtmp key")
	}

	// port
	if len(u.Port()) == 0 {
		u.Host = net.JoinHostPort(u.Hostname(), "1935")
	}

	var host, port string
	host, port, err = net.SplitHostPort(u.Host)
	if err != nil {
		return err
	}

	portNumber, portErr := strconv.Atoi(port)
	if portErr != nil || portNumber < 1 || portNumber > 65535 {
		return errutil.New("invalid rtmp port=" + port)
	}

	resolve := s.resolveIP
	if resolve == nil {
		resolve = net.DefaultResolver.LookupIPAddr
	}
	_, err = resolve(ctx, host)
	if err != nil {
		return err
	}

	*rtmp = u.String()
	return nil
}

type relayKey struct {
	pull string
	push string
}

func (s *Server) Close() {
	s.closeQueries()
	s.lockRelay(context.Background())
	defer s.unlockRelay()
	s.closed = true
	s.Session.Range(func(key, value any) bool {
		session := value.(*core.DynamicRelay)
		session.Stop()
		<-session.Wait()
		s.Session.CompareAndDelete(key, session)
		return true
	})
}

func (s *Server) lockRelay(ctx context.Context) bool {
	s.relayMu.Lock()
	if s.relayGate == nil {
		s.relayGate = make(chan struct{}, 1)
	}
	gate := s.relayGate
	s.relayMu.Unlock()
	select {
	case gate <- struct{}{}:
		if ctx.Err() != nil {
			<-gate
			return false
		}
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Server) unlockRelay() { <-s.relayGate }

/*
	relay 转播视频

请求参数: ?action=[start|stop]
[Post]:

	pull:rtmp://{host}/{app}/{instance}
	push:rtmp://{host}/{app}/{instance}
*/
func (s *Server) relay(w http.ResponseWriter, req *http.Request) {
	ww := httpx.NewWriter(w)

	if !config.Get().Console.EnableRelay {
		ww.QuickSendJSON(http.StatusUnauthorized, "Frozen relay api", nil)
		return
	}

	if req.PathValue("action") != "start" && req.PathValue("action") != "stop" {
		ww.QuickSendJSON(http.StatusBadRequest,
			"invalid action,support 'start' and 'stop' only", nil)
		return
	}

	// post: pull
	pull := req.PostFormValue("pull")
	err := s.checkRtmpURLContext(req.Context(), &pull)
	if err != nil {
		more := errutil.Wrapf(err, "post(pull) is invalid,pull='%s'", pull)
		ww.QuickSendJSON(http.StatusBadRequest, more.Error(), nil)
		return
	}

	// post: push
	push := req.PostFormValue("push")
	err = s.checkRtmpURLContext(req.Context(), &push)
	if err != nil {
		more := errutil.Wrapf(err, "post(push) is invalid,push='%s'", push)
		ww.QuickSendJSON(http.StatusBadRequest, more.Error(), nil)
		return
	}

	if pull == push {
		ww.QuickSendJSON(http.StatusBadRequest, "same url to relay,post=pull", nil)
		return
	}
	if !s.lockRelay(req.Context()) {
		return
	}
	defer s.unlockRelay()
	if s.closed {
		ww.QuickSendJSON(http.StatusServiceUnavailable, "console is shutting down", nil)
		return
	}
	key := relayKey{pull: pull, push: push}

	switch req.PathValue("action") {
	case "stop":
		value, ok := s.Session.Load(key)
		if !ok {
			ww.QuickSendJSON(0, "nothing to stop", nil)
			return
		}

		session := value.(*core.DynamicRelay)
		session.Stop()
		<-session.Wait()
		s.Session.CompareAndDelete(key, session)

		ww.QuickSendJSON(0, "stop successfully", nil)
		return
	case "start":
		var session *core.DynamicRelay

		value, ok := s.Session.Load(key)
		if !ok || value.(*core.DynamicRelay).IsDone() {
			session = core.NewDynamicRelay(pull, push, time.Duration(config.Get().RTMP.DialTimeout)*time.Second)
			if session == nil {
				ww.QuickSendJSON(http.StatusBadRequest, "relay failed", nil)
				return
			}
		} else {
			session = value.(*core.DynamicRelay)
		}

		if session.IsDone() {
			err = session.StartContext(req.Context())
			if err != nil {
				ww.QuickSendJSON(http.StatusBadGateway, err.Error(), nil)
				return
			}

			s.Session.Store(key, session)
			finished := session.Wait()

			// 等待服务结束后自动清理
			go func() {
				<-finished
				s.Session.CompareAndDelete(key, session)
				logging.Debug("Clear session: '", pull, "' TO '", push, "'")
			}()
		}

		ww.QuickSendJSON(0, "start successfully", nil)
		return
	}
}

// config 获取当前配置
//
// [GET]
func (s *Server) config(w http.ResponseWriter, _ *http.Request) {
	httpx.NewWriter(w).QuickSendJSON(0, "OK", config.Get().Redacted())
}
