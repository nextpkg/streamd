package config

import (
	"testing"

	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/stretchr/testify/assert"
)

func newAppRecorderCfg() *AppRecorder {
	return &AppRecorder{
		Enable:   true,
		SaveTo:   "/tmp/streamd",
		SaveAs:   "flv",
		SaveMode: "round",
		Driver:   "fs",
		Segment:  900,
		Event:    RecorderEvent{},
		CosCfg: &objectstore.COSConfig{
			Enable:    true,
			Address:   "example-123.ap-test",
			SecretID:  "abc",
			SecretKey: "def",
		},
	}
}

func TestAppRecorder_Validate(t *testing.T) {
	at := assert.New(t)

	cfg := newAppRecorderCfg()
	at.Nil(cfg.validate())
}

func TestAppRecorder_Normalize(t *testing.T) {
	at := assert.New(t)

	r := newAppRecorderCfg()
	at.Equal(int64(12345300), r.Normalize(12345567))
}
