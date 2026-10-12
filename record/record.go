package record

import (
	"errors"
	"sync"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/rtmp/core"
)

// Stream is recorder streaming process
type Stream struct {
	*core.Funnel
}

// Server is recorder server
type Server struct {
	conn   sync.Map
	mu     sync.Mutex
	closed bool
}

// NewServer is recorder server
func NewServer() *Server {
	return &Server{}
}

// GetWriter is the writer of recorder,IsExternal=false
func (s *Server) GetWriter(info *base.Info, connect *base.ConnectInfo) (base.WriteCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("recorder is closed")
	}
	if config.Get().GetApp(info.App) == nil {
		return nil, errors.New("recording application is not configured")
	}

	previous, loaded := s.conn.Load(info.Key)
	if loaded {
		stream := previous.(*Stream)
		if !stream.IsClosed() {
			return nil, nil
		}
		stream.Wait()
	}
	stream := &Stream{}

	stream.Funnel = core.NewFunnel(newSwitchingWriter(info, connect))
	s.conn.Store(info.Key, stream)

	go func() {
		stream.Wait()
		s.conn.CompareAndDelete(info.Key, stream)
	}()

	return stream, nil
}

// Close Server
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	var wg []*Stream

	s.conn.Range(func(key, value interface{}) bool {
		s.conn.Delete(key)

		stream := value.(*Stream)
		stream.Close()

		wg = append(wg, stream)

		return true
	})

	for _, v := range wg {
		v.Wait()
	}
}
