package rtmp

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/metrics"
)

func TestChannelGaugeMatchesConcurrentRegistrationAndRetirement(test *testing.T) {
	metrics.SetCluster("gauge-order")
	defer metrics.SetCluster("")
	baseline := online.Load()
	stream := &Stream{info: &base.Info{IsExternal: true}}
	check := func(expected int64) {
		response := httptest.NewRecorder()
		metrics.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		value := fmt.Sprintf("streamd_channels{app=\"\",channel=\"\",cluster=\"gauge-order\"} %d\n", expected)
		if online.Load() != expected || !strings.Contains(response.Body.String(), value) {
			test.Errorf("channel gauge disagrees with completed lifecycle: want %d", expected)
		}
	}
	for range 50 {
		var workers sync.WaitGroup
		for range 256 {
			workers.Go(stream.register)
		}
		workers.Wait()
		check(baseline + 256)
		for range 256 {
			workers.Go(stream.deregister)
		}
		workers.Wait()
		check(baseline)
		if test.Failed() {
			return
		}
	}
}
