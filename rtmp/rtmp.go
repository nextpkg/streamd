package rtmp

import (
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/config"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/rtmp/core"
)

// Server RTMP服务
type Server struct {
	currentConfig func() *config.Config

	handler   Handler
	players   []EmbedWriter
	mutex     sync.Mutex
	closed    bool
	closeOnce sync.Once
	workers   sync.WaitGroup
	listeners map[net.Listener]struct{}
	pending   map[net.Conn]struct{}
}

// NewRtmpServer RTMP服务
func NewRtmpServer(h Handler, w ...EmbedWriter) *Server {
	return newRtmpServer(h, config.Get, w...)
}

func newRtmpServer(h Handler, currentConfig func() *config.Config, w ...EmbedWriter) *Server {
	return &Server{
		handler:       h,
		currentConfig: currentConfig,
		players:       w,
	}
}

// Serve RTMP服务
func (s *Server) Serve(listener net.Listener) {
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		_ = listener.Close()
		return
	}
	if s.listeners == nil {
		s.listeners = make(map[net.Listener]struct{})
		s.pending = make(map[net.Conn]struct{})
	}
	s.listeners[listener] = struct{}{}
	s.workers.Add(1)
	s.mutex.Unlock()
	defer func() {
		s.mutex.Lock()
		delete(s.listeners, listener)
		s.mutex.Unlock()
		s.workers.Done()
	}()
	for {
		netConn, err := listener.Accept()
		if err != nil {
			log.Trace(err)
			log.Info("serve exit")
			return
		}

		s.mutex.Lock()
		if s.closed {
			s.mutex.Unlock()
			_ = netConn.Close()
			return
		}
		s.pending[netConn] = struct{}{}
		s.workers.Add(1)
		s.mutex.Unlock()
		// Transport settings belong to this accepted connection, even across reloads.
		settings := s.currentConfig().RTMP
		go func() {
			defer func() {
				s.mutex.Lock()
				delete(s.pending, netConn)
				s.mutex.Unlock()
				s.workers.Done()
			}()
			conn := core.NewConn(netConn, settings.BufSize)
			conn.InitSlab(settings.SlabMin*1024, settings.SlabMax*1024)
			s.handleConn(conn, settings)
		}()
	}
}

func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.mutex.Lock()
		s.closed = true
		listeners := make([]net.Listener, 0, len(s.listeners))
		for listener := range s.listeners {
			listeners = append(listeners, listener)
		}
		connections := make([]net.Conn, 0, len(s.pending))
		for connection := range s.pending {
			connections = append(connections, connection)
		}
		s.mutex.Unlock()
		for _, listener := range listeners {
			_ = listener.Close()
		}
		for _, connection := range connections {
			_ = connection.Close()
		}
		s.workers.Wait()
	})
}

// handleConn 处理客户端到RTMP服务的请求
func (s *Server) handleConn(conn *core.Conn, settings config.RTMP) {
	handedOff := false
	defer func() {
		if !handedOff {
			_ = conn.Close()
		}
		if e := recover(); e != nil {
			log.Error(e)
			debug.PrintStack()
		}
	}()

	// 1. 和客户端握手
	err := conn.HandshakeServer()
	if err != nil {
		s.mutex.Lock()
		closed := s.closed
		s.mutex.Unlock()
		if !closed {
			log.Error(err)
		}
		return
	}

	if err := conn.SetDeadline(time.Now().Add(time.Duration(settings.ReadTimeout) * time.Second)); err != nil {
		log.Debug(err)
		return
	}
	server := core.NewConnServer(conn, settings.ChunkSize)

	// 2. 交互控制指令
	err = server.CommandLinkup()
	if err != nil {
		log.Debug(err)
		return
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		log.Debug(err)
		return
	}

	// Application admission uses the latest snapshot after negotiation.
	app := s.currentConfig().GetApp(server.GetConnect().App)
	if app == nil {
		log.Infof("application is not configured,app='%s'", server.GetConnect().App)
		return
	}

	log.Infof("[%s/%s]New %s from %s",
		server.GetConnect().App, server.GetPublish().Name,
		strings.ToUpper(server.Method()), conn.Conn.RemoteAddr())

	// 3. play或者publish
	switch server.Method() {
	case core.Publish:
		reader := core.NewReader(server)

		// s.player用于接收reader数据的拷贝
		s.handler.InstallReader(reader)

		// player在断线后定时重启
		for _, player := range s.players {
			if player == nil {
				continue
			}

			writer, err := player.GetWriter(reader.Info().Copy(), server.GetConnect())
			if err != nil {
				log.Errorf("[%s/%s]Please wait for ten seconds because player returned: %s",
					server.GetConnect().App, server.GetPublish().Name, err)
				continue
			}
			if writer == nil {
				continue
			}

			// External=false
			s.handler.InstallWriter(writer)
		}
	case core.Play:
		s.handler.InstallWriter(core.NewWriter(server))
	default:
		log.Error("only support PUBLISH and PLAY command")
		return
	}
	handedOff = true
}
