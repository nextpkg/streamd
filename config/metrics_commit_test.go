package config

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nextpkg/streamd/metrics"
)

func TestMetricsClusterFollowsOnlyCommittedConfiguration(test *testing.T) {
	filename := filepath.Join(test.TempDir(), "config.yaml")
	write := func(body string) {
		test.Helper()
		if err := os.WriteFile(filename, []byte(body), 0600); err != nil {
			test.Fatal(err)
		}
	}
	check := func(cluster string) {
		test.Helper()
		metrics.GaugeGlobal(metrics.Channels, 2)
		response := httptest.NewRecorder()
		metrics.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		if !strings.Contains(response.Body.String(), `streamd_channels{app="",channel="",cluster="`+cluster+`"} 2`) {
			test.Errorf("metrics do not reflect committed cluster %q", cluster)
		}
	}
	write("metrics:\n  cluster: initial\n")
	manager, err := NewManager(context.Background(), filename)
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { Get().Revoke(); metrics.SetCluster("") })
	check("initial")
	write("metrics:\n  cluster: rejected\nrtmp:\n  addr: 127.0.0.1:29999\n")
	if err := manager.Reload(); err == nil {
		test.Fatal("incompatible candidate accepted")
	}
	check("initial")
	write("metrics:\n  cluster: updated\n")
	if err := manager.Reload(); err != nil {
		test.Fatal(err)
	}
	check("updated")
}
