package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newAppCfg() *App {
	return &App{
		Enable:     true,
		AppName:    "source",
		StaticPush: []string{"rtmp://127.0.0.1/push"},
		StaticPull: []string{"rtmp://127.0.0.1/pull"},
		HLS:        *newHLSCfg(),
		Event:      *NewAppEventCfg(),
		Recorder:   *newAppRecorderCfg(),
	}
}

func TestApp_Validate(t *testing.T) {
	at := assert.New(t)

	cfg := newAppCfg()
	at.Nil(cfg.validate())
}

func TestApp_StaticRTMP(t *testing.T) {
	at := assert.New(t)

	cfg := newAppCfg()
	at.Equal("rtmp://127.0.0.1/source/t_ins", cfg.StaticRtmp("127.0.0.1", "t_ins"))
}

func TestApp_StaticPullList(t *testing.T) {
	at := assert.New(t)

	cfg := newAppCfg()
	at.Equal([]string{"rtmp://127.0.0.1/pull/t_ins"}, cfg.StaticPullList("t_ins"))
}

func TestApp_StaticPushList(t *testing.T) {
	at := assert.New(t)

	cfg := newAppCfg()
	at.Equal([]string{"rtmp://127.0.0.1/push/t_ins"}, cfg.StaticPushList("t_ins"))
}
