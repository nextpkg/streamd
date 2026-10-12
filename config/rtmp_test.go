package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newRTMPCfg() *RTMP {
	return &RTMP{
		Addr:        "127.0.0.1:1935",
		Gop:         1,
		ChunkSize:   1024,
		SlabMin:     0,
		SlabMax:     0,
		BufSize:     4096,
		EdgeAlive:   60,
		EdgeCheck:   30,
		ReadTimeout: 60,
	}
}

func TestRTMP_validate(t *testing.T) {
	at := assert.New(t)

	cfg := newRTMPCfg()
	at.Nil(cfg.validate())
}
