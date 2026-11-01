package console

import (
	"net/http"

	"github.com/nextpkg/streamd/internal/httpx"
	"github.com/nextpkg/streamd/rtmp"
)

// statKeys 获取当前的keys列表
//
// [GET]
func (s *Server) statKeys(w http.ResponseWriter, _ *http.Request) {
	ww := httpx.NewWriter(w)

	rtmpConn := s.Handler.(*rtmp.Conn)
	keys := rtmpConn.StreamKeys()

	ret := &publisherKeys{
		Count: len(keys),
		List:  keys,
	}

	ww.QuickSendJSON(0, "OK", ret)
}

// statPubInfo 统计发布流的连接状态
//
// [GET]请求参数: key=app/instance
func (s *Server) statPubInfo(w http.ResponseWriter, req *http.Request) {
	ww := httpx.NewWriter(w)

	// Get: key
	key := req.URL.Query().Get("key")
	if len(key) == 0 {
		ww.QuickSendJSON(http.StatusBadRequest, "missing key", nil)
		return
	}

	rtmpConn := s.Handler.(*rtmp.Conn)
	reader := rtmpConn.Reader(key)

	ret := &publisherInfo{
		Count: 0,
	}

	if reader != nil {
		ret.Count = 1
		ret.List = append(ret.List, streamInfo{
			Key:           key,
			Pts:           reader.LastVideoTime(),
			Duration:      reader.Duration(),
			VideoReceived: reader.VideoLen(), VideoSpeed: reader.VideoBps(),
			AudioReceived: reader.AudioLen(), AudioSpeed: reader.AudioBps(),
		})
	}

	ww.QuickSendJSON(0, "OK", ret)
}

// statSubInfo 统计订阅连接的数据信息
// [GET]请求参数: key=app/instance
func (s *Server) statSubInfo(w http.ResponseWriter, req *http.Request) {
	ww := httpx.NewWriter(w)

	// Get: key
	key := req.URL.Query().Get("key")
	if len(key) == 0 {
		ww.QuickSendJSON(http.StatusBadRequest, "missing key", nil)
		return
	}

	rtmpConn := s.Handler.(*rtmp.Conn)
	writer := rtmpConn.Writer(key)

	ret := subscriberInfo{
		Count: len(writer),
	}

	for _, v := range writer {
		ret.List = append(ret.List, streamInfo{
			Key:           key,
			Pts:           v.LastVideoTime(),
			Duration:      v.Duration(),
			VideoReceived: v.VideoLen(), VideoSpeed: v.VideoBps(),
			AudioReceived: v.AudioLen(), AudioSpeed: v.AudioBps(),
		})
	}

	ww.QuickSendJSON(0, "OK", ret)
}

// statSubOnline 统计订阅者的数量
//
// [GET]请求参数: key=app/instance
func (s *Server) statSubOnline(w http.ResponseWriter, req *http.Request) {
	ww := httpx.NewWriter(w)
	conn := s.Handler.(*rtmp.Conn)

	// Get: key
	key := req.URL.Query().Get("key")
	if len(key) == 0 {
		ww.QuickSendJSON(http.StatusBadRequest, "missing key", nil)
		return
	}

	count, checkIn := conn.WriterActive(key)
	ret := &subscriberOnline{
		Count:   int(count),
		Checkin: checkIn,
	}
	ww.QuickSendJSON(0, "with-key", ret)
}
