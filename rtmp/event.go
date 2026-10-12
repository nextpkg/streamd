package rtmp

import (
	"encoding/json"
	"time"

	"github.com/nextpkg/streamd/internal/httpx"
	log "github.com/nextpkg/streamd/internal/logging"
)

type eventFunc func()

// 用户事件
type event struct {
	onPublish     eventFunc
	onPublishDone eventFunc
}

// 触发事件
func fireEvent(name, url, key string) eventFunc {
	if len(name) == 0 || len(url) == 0 || len(key) == 0 {
		return func() {}
	}

	return func() {
		data, err := json.Marshal(map[string]interface{}{
			"event": name,
			"key":   key,
			"occur": time.Now().UnixNano() / int64(time.Millisecond),
		})
		if err != nil {
			log.Error(err)
			return
		}

		// 发送post请求
		report := func(failure error) {
			log.Errorf("[%s]err=%s,event=%s", key, failure, name)
		}
		if err := httpx.SubmitJSON(url, data, report); err != nil {
			report(err)
		}
	}
}
