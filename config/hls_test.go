package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newHLSCfg() *AppHLS {
	return &AppHLS{
		Enable:         true,
		Fragment:       900,
		PlaylistLength: 4,
		SecureKey:      "abc",
		SecureTTL:      12345,
	}
}

func TestAppHLS_AntiLeechQuery(t *testing.T) {
	at := assert.New(t)
	cfg := newHLSCfg()
	at.Equal("token=", cfg.AntiLeechQuery("abc/def")[:6])
}

func TestAppHLS_validate(t *testing.T) {
	at := assert.New(t)

	cfg := newHLSCfg()
	at.Nil(cfg.validate())
}
