package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewAppEventCfg() *AppEvent {
	return &AppEvent{
		OnPublish:     "http://127.0.0.1/on/publish",
		OnPublishDone: "http://127.0.0.1/on/publish/done",
	}
}

func NewRecordEventCfg() *RecorderEvent {
	return &RecorderEvent{
		OnRecord:     "http://127.0.0.1/on/record",
		OnRecordDone: "http://127.0.0.1/on/record/done",
	}
}

func TestAppEvent_validate(t *testing.T) {
	at := assert.New(t)

	cfg := NewAppEventCfg()
	at.Nil(cfg.validate())
}

func TestRecordEvent_validate(t *testing.T) {
	at := assert.New(t)

	cfg := NewRecordEventCfg()
	at.Nil(cfg.load())
}
