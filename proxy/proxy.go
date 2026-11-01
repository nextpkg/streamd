package proxy

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/logging"
)

type configurationKey struct{}

type Server struct {
	*http.ServeMux
	dispatcher      *http.ServeMux
	mutex           sync.Mutex
	closed          bool
	done            chan struct{}
	requests        map[*responseWriter]struct{}
	transport       *http.Transport
	responseTimeout time.Duration
}

type proxyLog struct{}

func (proxyLog) Write(message []byte) (int, error) {
	logging.Warn("proxy transport: ", strings.TrimSpace(string(message)))
	return len(message), nil
}

func Init(routers ...base.Router) *Server {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 10 * time.Second
	router := &Server{ServeMux: http.NewServeMux(), dispatcher: http.NewServeMux(), done: make(chan struct{}), requests: make(map[*responseWriter]struct{}), transport: transport}
	for _, child := range routers {
		child.Init(router.ServeMux)
	}
	forward := http.NewServeMux()
	forward.HandleFunc("GET /hfs/{app}/{file}", router.relay)
	forward.HandleFunc("GET /hls/{app}/{file}", router.relay)
	forward.HandleFunc("GET /hls/{app}/{instance}/{file}", router.relay)
	router.dispatcher.HandleFunc("/", func(response http.ResponseWriter, request *http.Request) {
		cfg := config.Get()
		escapedPrefix := mediaPrefix(request.URL.EscapedPath())
		prefix, err := url.PathUnescape(escapedPrefix)
		if err != nil || prefix == "" || !cfg.Proxy.AcceptsPrefix(prefix) {
			router.ServeMux.ServeHTTP(response, request)
			return
		}
		writer := router.response(response, request.WithContext(context.WithValue(request.Context(), configurationKey{}, cfg)))
		if writer == nil {
			response.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer router.finishResponse(writer)
		forwarded := request.WithContext(writer.ctx)
		address := *request.URL
		address.Path = strings.TrimPrefix(address.Path, prefix)
		if address.RawPath != "" {
			address.RawPath = strings.TrimPrefix(address.RawPath, escapedPrefix)
		}
		forwarded.URL = &address
		forward.ServeHTTP(writer, forwarded)
	})
	return router
}

func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	server.dispatcher.ServeHTTP(response, request)
}

func mediaPrefix(path string) string {
	filename, err := url.PathUnescape(path[strings.LastIndex(path, "/")+1:])
	if err != nil {
		return ""
	}
	components := 3
	family := "hls"
	switch {
	case strings.HasSuffix(filename, ".ts"):
		components = 4
	case strings.HasSuffix(filename, ".m3u8"):
	case strings.HasSuffix(filename, ".flv"):
		family = "hfs"
	default:
		return ""
	}
	boundary := len(path)
	for component := 0; component < components; component++ {
		boundary = strings.LastIndex(path[:boundary], "/")
		if boundary <= 0 {
			return ""
		}
	}
	segmentEnd := boundary + 1 + strings.Index(path[boundary+1:], "/")
	segment, err := url.PathUnescape(path[boundary+1 : segmentEnd])
	if err != nil || segment != family {
		return ""
	}
	return path[:boundary]
}

func (server *Server) relay(response http.ResponseWriter, request *http.Request) {
	instance := request.PathValue("instance")
	filename := request.PathValue("file")
	if instance == "" {
		suffix := ".m3u8"
		if strings.HasPrefix(request.URL.Path, "/hfs/") {
			suffix = ".flv"
		}
		var matched bool
		instance, matched = strings.CutSuffix(filename, suffix)
		if !matched || instance == "" {
			http.NotFound(response, request)
			return
		}
	} else if !strings.HasSuffix(filename, ".ts") || filename == ".ts" {
		http.NotFound(response, request)
		return
	}
	cfg := request.Context().Value(configurationKey{}).(*config.Config)
	settings := &cfg.Proxy
	key := request.PathValue("app") + "/" + instance
	if status := settings.Check(key, request.URL.Query().Get("token"), request.URL.Query().Get("e")); status != http.StatusOK {
		response.WriteHeader(status)
		return
	}
	backend := cfg.Finder.GetBackend(request.PathValue("app"), instance)
	if backend == "" {
		response.WriteHeader(http.StatusNotFound)
		if settings.OnNotFound != "" {
			address, err := url.Parse(settings.OnNotFound)
			if err != nil {
				return
			}
			query := address.Query()
			query.Add("app", request.PathValue("app"))
			query.Add("instance", instance)
			address.RawQuery = query.Encode()
			callback, err := http.NewRequestWithContext(request.Context(), http.MethodGet, address.String(), nil)
			if err != nil {
				return
			}
			client := &http.Client{Transport: server.transport, Timeout: 3 * time.Second}
			result, err := client.Do(callback)
			if err != nil {
				if request.Context().Err() == nil {
					logging.Error("proxy callback: ", err)
				}
				return
			}
			defer func() { _ = result.Body.Close() }()
			if _, err := io.Copy(response, io.LimitReader(result.Body, 1024*1024)); err != nil && request.Context().Err() == nil {
				logging.Error("proxy callback body: ", err)
			}
		}
		return
	}
	endpoint, err := config.DiscoveryEndpoint(backend, "http")
	if err != nil {
		logging.Error("invalid HTTP discovery endpoint: ", err)
		http.Error(response, "source record unavailable", http.StatusBadGateway)
		return
	}
	address := &url.URL{Scheme: "http", Host: endpoint}
	forward := httputil.NewSingleHostReverseProxy(address)
	forward.Transport = server.transport
	forward.ErrorLog = log.New(proxyLog{}, "", 0)
	forward.FlushInterval = -1
	forward.ErrorHandler = func(response http.ResponseWriter, request *http.Request, err error) {
		if request.Context().Err() != nil {
			return
		}
		logging.Error("upstream HTTP failure: ", err)
		http.Error(response, "upstream unavailable", http.StatusBadGateway)
	}
	forward.ServeHTTP(response, request)
}

func (server *Server) Close() {
	server.mutex.Lock()
	if server.closed {
		server.mutex.Unlock()
		<-server.done
		return
	}
	server.closed = true
	requests := make([]*responseWriter, 0, len(server.requests))
	for request := range server.requests {
		requests = append(requests, request)
	}
	server.mutex.Unlock()
	for _, request := range requests {
		request.abort()
	}
	for _, request := range requests {
		<-request.done
	}
	server.transport.CloseIdleConnections()
	close(server.done)
}
