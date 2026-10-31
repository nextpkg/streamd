package storage

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/metrics"
)

func TestRecordingGaugeMatchesConcurrentFileRetirement(test *testing.T) {
	metrics.SetCluster("gauge-order")
	defer metrics.SetCluster("")
	baseline := metrics.AddGlobal(metrics.Recording, 0)
	for range 50 {
		var workers sync.WaitGroup
		stores := make([]*Storage, 256)
		for index := range stores {
			stores[index] = &Storage{cfg: &config.AppRecorder{Driver: "fs"}, dr: &resumeDriver{}, event: NewEvent(), info: &base.Info{App: "metric", Instance: "sample"}, cTime: time.Now()}
			stores[index].isOpen.Store(true)
			metrics.AddGlobal(metrics.Recording, 1)
		}
		for _, store := range stores {
			workers.Go(func() {
				if err := store.Close(true); err != nil {
					test.Error(err)
				}
				if err := store.Close(true); err != nil {
					test.Error(err)
				}
			})
		}
		workers.Wait()
		response := httptest.NewRecorder()
		metrics.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		value := fmt.Sprintf("streamd_recording{app=\"\",channel=\"\",cluster=\"gauge-order\"} %g\n", baseline)
		if !strings.Contains(response.Body.String(), value) {
			test.Fatal("recording gauge disagrees with completed file retirement")
		}
	}
}
