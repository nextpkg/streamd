package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newPluginCfg() *Config {
	return &Config{
		Console: Console{
			EnableRelay: false,
		},
		Metadata: Metadata{
			Provider: "streamd",
			Service:  "Streaming media process service",
		},
		Finder: *cfgFinder,
		RTMP:   *newRTMPCfg(),
		Proxy:  Proxy{Prefix: "live"},
		App:    []App{*newAppCfg()},
		app:    make(map[string]*App),
	}
}

func TestPlugin_Validate(t *testing.T) {
	at := assert.New(t)

	cfg := newPluginCfg()
	at.Nil(cfg.Validate())
}

func TestPlugin_App(t *testing.T) {
	at := assert.New(t)

	cfg := newPluginCfg()

	app := newAppCfg()
	cfg.SetApp(app.AppName, app)
	at.Equal(app, cfg.GetApp(app.AppName))

	at.Equal(&app.Recorder, cfg.GetRecorder(app.AppName))
	at.Equal(&app.HLS, cfg.GetHLS(app.AppName))
	at.Equal(app.StaticPush, cfg.GetStaticPush(app.AppName))
	at.Equal(app.StaticPull, cfg.GetStaticPull(app.AppName))
}
