package config

import (
	"fmt"
)

// AppEvent 直播事件
type AppEvent struct {
	OnPublish     string `json:"on_publish"`
	OnPublishDone string `json:"on_publish_done"`
}

func (e *AppEvent) validate() error {
	// OnPublish
	if len(e.OnPublish) > 0 && !isHTTP(e.OnPublish) {
		return fmt.Errorf("invalid `on_publish`=%s,support http(s) only", e.OnPublish)
	}

	// OnPublishDone
	if len(e.OnPublishDone) > 0 && !isHTTP(e.OnPublishDone) {
		return fmt.Errorf("invalid `on_publish_done`=%s,support http(s) only", e.OnPublish)
	}

	return nil
}

// RecorderEvent 录制事件
type RecorderEvent struct {
	OnRecord     string `json:"on_record"`      // 录制开始触发回调
	OnRecordDone string `json:"on_record_done"` // 录制落地触发回调
}

func (e *RecorderEvent) load() error {
	// OnRecord
	if len(e.OnRecord) > 0 && !isHTTP(e.OnRecord) {
		return fmt.Errorf("invalid `on_record`=%s,support http(s) scheme only", e.OnRecord)
	}

	// OnRecordDone
	if len(e.OnRecordDone) > 0 && !isHTTP(e.OnRecordDone) {
		return fmt.Errorf("invalid `on_record_done`=%s url,support http(s) scheme only", e.OnRecordDone)
	}

	return nil
}
