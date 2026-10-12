package hls

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

func TestHLSConfigurationAppliesAtVideoBoundary(t *testing.T) {
	info := base.NewInfo(t.Name(), "sample", false)
	initial := &config.App{Enable: true, AppName: info.App, HLS: config.AppHLS{Fragment: 2, PlaylistLength: 2}}
	config.Get().SetApp(info.App, initial)
	funnel := NewStream(info)
	stream := funnel.Terminal.(*Stream)
	defer func() { stream.Close(); funnel.Close(); funnel.Wait() }()
	next := *initial
	next.HLS.Enable, next.HLS.Fragment, next.HLS.PlaylistLength = true, 4, 4
	config.Get().SetApp(info.App, &next)
	frame := &packet.Packet{Type: packet.PktVideo, Baseline: 2000, Data: []byte{0x27, 1, 0, 0, 0, 0, 0, 0, 1, 0x41}}
	require.NoError(t, flv.NewDemuxer().Demux(frame))
	require.NoError(t, stream.Write(frame))
	require.False(t, stream.active.Load())
	require.Equal(t, int64(2), stream.param.Fragment)
	frame.Data[0], frame.Data[9] = 0x17, 0x65
	require.NoError(t, flv.NewDemuxer().Demux(frame))
	require.NoError(t, stream.Write(frame))
	require.True(t, stream.active.Load())
	require.Equal(t, int64(4), stream.param.Fragment)
	require.Equal(t, int64(4), stream.param.PlaylistLength)
	removed := config.NewConfig()
	require.NoError(t, removed.Validate())
	stream.applyConfiguration(removed)
	require.True(t, stream.active.Load(), "removing an app must retain existing live media")
	disabled := next
	disabled.HLS.Enable = false
	config.Get().SetApp(info.App, &disabled)
	require.NoError(t, stream.Write(frame))
	require.False(t, stream.active.Load())
}

func TestHLSBoundaryRetainsSigningKeys(t *testing.T) {
	info := base.NewInfo(t.Name(), "sample", false)
	initial := &config.App{Enable: true, AppName: info.App, HLS: config.AppHLS{
		Enable: true, Fragment: 2, PlaylistLength: 2, SecureKey: "old", SecureTTL: 120}}
	config.Get().SetApp(info.App, initial)
	funnel := NewStream(info)
	stream := funnel.Terminal.(*Stream)
	defer func() { stream.Close(); funnel.Close(); funnel.Wait() }()
	expires := time.Now().Unix() + 100
	token := config.AntiLeechSign(info.Key, "old", expires)
	next := *initial
	next.HLS.SecureKey, next.HLS.SecureTTL = "new", 20
	config.Get().SetApp(info.App, &next)
	stream.applyConfiguration(config.Get())
	require.Equal(t, http.StatusOK, stream.CheckSignature(token, strconv.FormatInt(expires, 10)))
	require.Equal(t, http.StatusForbidden, stream.CheckSignature("forged", strconv.FormatInt(expires, 10)))
}
