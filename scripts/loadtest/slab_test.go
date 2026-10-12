package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlabAnalyzerDeduplicatesClosureWithoutChangingCounters(test *testing.T) {
	line := `{"level":"DEBUG-4","msg":"Connection from 127.0.0.1:12345 is closed,slab stat,max:2,medium:3,min:4"}`
	counts, err := slabCounts(strings.NewReader(line + "\n" + line + "\n"))
	require.NoError(test, err)
	require.Equal(test, [3]int64{2, 3, 4}, counts)
}

func TestSlabAnalyzerRejectsMissingConflictingAndFailedMeasurements(test *testing.T) {
	valid := `{"level":"DEBUG-4","msg":"Connection from 127.0.0.1:12345 is closed,slab stat,max:2,medium:3,min:4"}`
	for _, body := range []string{"", "not-json", `{"level":"ERROR","msg":"failure"}`, `{"msg":"slab stat,max:x"}`, valid + "\n" + strings.ReplaceAll(valid, "min:4", "min:5"), valid + "\n" + strings.ReplaceAll(valid, ":12345", ":12346"), strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(valid, "max:2", "max:0"), "medium:3", "medium:0"), "min:4", "min:0")} {
		_, err := slabCounts(strings.NewReader(body))
		require.Error(test, err, body)
	}
}

func TestSlabAnalyzerIsolatesOverridesWithoutChangingParentEnvironment(test *testing.T) {
	test.Setenv("STREAMD_HTTP_ADDR", "0.0.0.0:1")
	test.Setenv("STREAMD_RTMP_ADDR", "0.0.0.0:2")
	test.Setenv("STREAMD_LOG_LEVEL", "ERROR")
	test.Setenv("STREAMD_LOADTEST_SENTINEL", "keep")
	environment := strings.Join(isolatedEnvironment(), "\n")
	require.NotContains(test, environment, "STREAMD_HTTP_ADDR=")
	require.NotContains(test, environment, "STREAMD_RTMP_ADDR=")
	require.NotContains(test, environment, "STREAMD_LOG_LEVEL=")
	require.Contains(test, environment, "STREAMD_LOADTEST_SENTINEL=keep")
}
