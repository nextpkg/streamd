package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProxyLifecycleMetricsHaveBoundedCardinality(test *testing.T) {
	registry := NewRegistry("test", 1)
	registry.ProxyRequestStarted()
	registry.ProxyRequestStarted()
	registry.ProxyRequestStarted()
	registry.ProxyRequestFinished(false)
	registry.ProxyRequestFinished(true)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	for _, expected := range []string{"streamd_proxy_requests_active 1\n", "streamd_proxy_responses_aborted_total 1\n"} {
		if !strings.Contains(response.Body.String(), expected) {
			test.Errorf("missing metric: %s", expected)
		}
	}
	if len(registry.series) != 0 {
		test.Fatal("proxy lifecycle metrics allocated stream or peer labels")
	}
	registry.ProxyRequestFinished(false)
}
