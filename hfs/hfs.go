package hfs

import (
	"net/http"
	"strings"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/rtmp"
)

// Server HFS server struct
type Server struct {
	handler rtmp.Handler
}

// NewServer HFS server
func NewServer(h rtmp.Handler) base.Router {
	return &Server{
		handler: h,
	}
}

// Init HFS server serve
//
// HTTP-FLV播放格式: "http://{host}/hfs/{app}/{instance}.flv"
func (s *Server) Init(r *http.ServeMux) *http.ServeMux {
	r.HandleFunc("GET /hfs/{app}/{file}", s.handleConn)
	return r
}

// handleConn HFS handler
func (s *Server) handleConn(w http.ResponseWriter, r *http.Request) {
	// ===============================
	// 设置响应头
	// ===============================
	// 设置为允许跨域
	// w.Header().Set("Access-Control-Allow-Origin", "*")
	// ===============================

	// ===============================
	// 根据请求, 申请播放流
	// ===============================
	filename := r.PathValue("file")
	if !strings.HasSuffix(filename, ".flv") {
		http.NotFound(w, r)
		return
	}
	instance := strings.TrimSuffix(filename, ".flv")
	w.Header().Set("Content-Type", "video/x-flv")
	w.Header().Set("Cache-Control", "no-store")
	stream := NewStream(r.PathValue("app"), instance, w)
	stop := make(chan struct{})
	stopped := make(chan struct{})
	defer func() {
		close(stop)
		<-stopped
	}()
	go func() {
		defer close(stopped)
		select {
		case <-r.Context().Done():
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now())
			stream.Close()
		case <-stop:
		}
	}()

	s.handler.InstallWriter(stream)

	stream.Wait()

}
