package storage

import (
	"encoding/json"
	"path"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/internal/httpx"
	"github.com/nextpkg/streamd/internal/logging"
)

// EventFunc 事件函数类型
type EventFunc func(location string)

// Event 事件
type Event struct {
	OnRecord     EventFunc
	OnRecordDone EventFunc
}

// NewEvent 事件
func NewEvent() *Event {
	return &Event{
		OnRecord:     func(location string) {},
		OnRecordDone: func(location string) {},
	}
}

// FireEvent 业务相关的事件
func FireEvent(eventName, eventURL, eventKey string, conn *base.ConnectInfo) EventFunc {
	if len(eventName) == 0 || len(eventURL) == 0 || len(eventKey) == 0 {
		return func(location string) {}
	}

	return func(location string) {
		filename := path.Base(location)
		eventOccur := time.Now().UnixNano() / int64(time.Millisecond)
		data, err := json.Marshal(map[string]interface{}{
			"event":    eventName,
			"occur":    eventOccur,
			"key":      eventKey,
			"filename": filename,
			"location": location,
			"connInfo": conn,
		})
		if err != nil {
			logging.Error(err)
			return
		}

		report := func(failure error) {
			logging.Errorf("[%s]err=%s,event=%s", eventKey, failure, eventName)
		}
		if err := httpx.SubmitJSON(eventURL, data, report); err != nil {
			report(err)
		}
	}
}
