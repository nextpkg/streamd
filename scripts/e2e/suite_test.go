package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoreSuiteRecordsSuccessFailureAndCancellation(t *testing.T) {
	failure := errors.New("media validation failed")
	for _, mode := range []string{"success", "failure", "cancel-before", "cancel-during"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if mode == "cancel-before" {
				cancel()
			}
			var called []string
			err := executeSuite(ctx, root, "core", true, func(ctx context.Context, gotRoot, name string, race bool) (string, error) {
				require.Equal(t, root, gotRoot)
				require.True(t, race)
				called = append(called, name)
				if len(called) == 2 {
					if mode == "failure" {
						return "failed-evidence", failure
					}
					if mode == "cancel-during" {
						cancel()
						return "cancelled-evidence", ctx.Err()
					}
				}
				return name + "-evidence", nil
			})
			files, globErr := filepath.Glob(filepath.Join(root, "artifacts", "suite-core-*", "results.json"))
			require.NoError(t, globErr)
			require.Len(t, files, 1)
			body, readErr := os.ReadFile(files[0])
			require.NoError(t, readErr)
			var result suiteResult
			require.NoError(t, json.Unmarshal(body, &result))
			require.Len(t, result.Scenarios, 4)
			if mode == "success" {
				require.NoError(t, err)
				require.True(t, result.Complete)
				require.Equal(t, []string{"stream", "reload", "republish", "resume"}, called)
				for _, entry := range result.Scenarios {
					require.Equal(t, "passed", entry.Status)
					require.Equal(t, entry.Name+"-evidence", entry.Evidence)
				}
				return
			}
			require.False(t, result.Complete)
			if mode == "cancel-before" {
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, called)
				require.Equal(t, "cancelled", result.Scenarios[0].Status)
			} else {
				require.Equal(t, []string{"stream", "reload"}, called)
				require.Equal(t, "passed", result.Scenarios[0].Status)
				if mode == "failure" {
					require.ErrorIs(t, err, failure)
					require.Equal(t, "failed", result.Scenarios[1].Status)
					require.Equal(t, "failed-evidence", result.Scenarios[1].Evidence)
				} else {
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, "cancelled", result.Scenarios[1].Status)
				}
			}
			require.Equal(t, "not_run", result.Scenarios[2].Status)
			require.Equal(t, "not_run", result.Scenarios[3].Status)
		})
	}
}

func TestInvalidSelectionDoesNotStartWork(t *testing.T) {
	root := t.TempDir()
	_, err := execute(context.Background(), root, "unknown", false)
	require.ErrorContains(t, err, "unsupported scenario")
	err = executeSuite(context.Background(), root, "unknown", false, func(context.Context, string, string, bool) (string, error) {
		t.Fatal("invalid suite started a service")
		return "", nil
	})
	require.ErrorContains(t, err, "unsupported suite")
	_, err = os.Stat(filepath.Join(root, "artifacts"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStreamDurationBounds(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  int
		valid bool
	}{{"", 60, true}, {"45", 45, true}, {"60", 60, true}, {"44", 0, false}, {"bad", 0, false}, {"-1", 0, false}} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := streamDuration(tc.input)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.valid, err == nil)
		})
	}
}
