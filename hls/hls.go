package hls

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/rtmp/core"
)

// Server HLS服务器
type Server struct {
	conn            sync.Map
	mu              sync.Mutex
	closed          bool
	closeDone       chan struct{}
	responses       map[*responseWriter]struct{}
	responseTimeout time.Duration
}

// Init 启动HLS服务器(使用RTMP Relay提供数据源)
func (s *Server) Init(r *http.ServeMux) *http.ServeMux {
	r.HandleFunc("GET /hls/{app}/{file}", s.handleM3U8)
	r.HandleFunc("GET /hls/{app}/{ins}/{file}", s.handleTS)
	return r
}

// Close 清理连接
func (s *Server) Close() {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return
	}
	s.closed = true
	s.closeDone = make(chan struct{})
	defer close(s.closeDone)
	responses := make([]*responseWriter, 0, len(s.responses))
	for response := range s.responses {
		responses = append(responses, response)
	}
	var streams []*core.Funnel
	log.Debug("hls server is closing")
	s.conn.Range(func(key, value interface{}) bool {
		s.conn.Delete(key)

		streams = append(streams, value.(*core.Funnel))
		return true
	})
	s.mu.Unlock()
	for _, response := range responses {
		response.abort()
	}
	for _, stream := range streams {
		stream.Terminal.(*Stream).Close()
		stream.Close()
		stream.Wait()
	}
	for _, response := range responses {
		<-response.done
	}
	log.Debug("hls server is closed")
}

// GetWriter 从连接池里获取writer, 如果没有, 则插入一个新的连接
func (s *Server) GetWriter(info *base.Info, connect *base.ConnectInfo) (base.WriteCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("HLS server is closed")
	}
	if config.Get().GetApp(info.App) == nil {
		return nil, errors.New("HLS application is not configured")
	}

	previous, ok := s.conn.Load(info.Key)
	if ok {
		stream := previous.(*core.Funnel)
		if !stream.IsClosed() {
			return nil, nil
		}
		stream.Terminal.(*Stream).Close()
		stream.Wait()
	}

	stream := NewStream(info)
	s.conn.Store(info.Key, stream)

	go func() {
		stream.Wait()
		s.conn.CompareAndDelete(info.Key, stream)
	}()

	return stream, nil
}

// SetConnect 设置RTMP的Connect信息
func (s *Server) SetConnect(*base.ConnectInfo) {}

// [GET]"/app/ins.m3u8": 获取m3u8播放列表
func (s *Server) handleM3U8(w http.ResponseWriter, r *http.Request) {
	response := s.response(w, r)
	if response == nil {
		http.Error(w, "HLS server is closed", http.StatusServiceUnavailable)
		return
	}
	defer s.finishResponse(response)
	w = response
	filename := r.PathValue("file")
	if !strings.HasSuffix(filename, ".m3u8") {
		http.NotFound(w, r)
		return
	}
	r.SetPathValue("ins", strings.TrimSuffix(filename, ".m3u8"))

	// ===============================
	conn := s.getConn(r.PathValue("app") + "/" + r.PathValue("ins"))
	if conn == nil || !conn.Available() {
		http.Error(w, "m3u8 stream is not found", http.StatusNotFound)
		return
	}

	if window := conn.m3u8.Response(w); window > 0 && r.Method == http.MethodGet && r.Context().Err() == nil {
		conn.refreshViewer(time.Now(), window)
	}
}

// [GET]"/app/ins/seq.ts": 获取ts文件
func (s *Server) handleTS(w http.ResponseWriter, r *http.Request) {
	response := s.response(w, r)
	if response == nil {
		http.Error(w, "HLS server is closed", http.StatusServiceUnavailable)
		return
	}
	defer s.finishResponse(response)
	w = response
	filename := r.PathValue("file")
	if !strings.HasSuffix(filename, ".ts") {
		http.NotFound(w, r)
		return
	}
	r.SetPathValue("seq", strings.TrimSuffix(filename, ".ts"))

	// ===============================
	conn := s.getConn(r.PathValue("app") + "/" + r.PathValue("ins"))
	if conn == nil {
		http.Error(w, "ts stream is not found", http.StatusNotFound)
		return
	}

	item := conn.m3u8.GetAvailableItem(r.PathValue("seq"))
	if item == nil {
		http.Error(w, "lost ts segment: "+r.URL.Path, http.StatusNotFound)
		return
	}
	if item.RequiresSignature() {
		if status := conn.CheckSignature(r.URL.Query().Get("token"), r.URL.Query().Get("e")); status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
	}

	if item.Response(w) && r.Method == http.MethodGet && r.Context().Err() == nil {
		conn.refreshViewer(time.Now(), max(item.Duration(), conn.m3u8.Duration()))
	}
}

// 从连接池里取出匹配到的连接并返回
func (s *Server) getConn(key string) *Stream {
	v, ok := s.conn.Load(key)
	if !ok {
		return nil
	}

	sc := v.(*core.Funnel).Terminal
	return sc.(*Stream)
}
