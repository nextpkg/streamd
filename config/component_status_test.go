package config

import (
	"encoding/json"
	"testing"

	"github.com/nextpkg/streamd/internal/appstate"
	"github.com/stretchr/testify/require"
)

func TestRedactedIncludesRuntimeStatus(t *testing.T) {
	stream := t.Name()
	appstate.Start("recording", stream, "private-owner", 7, "active")
	t.Cleanup(func() { appstate.Forget("recording", stream, "private-owner") })
	appstate.ReportFailure("recording", stream, "private-owner", 7)
	body, err := json.Marshal(NewConfig().Redacted())
	require.NoError(t, err)
	var document struct {
		Components []map[string]any `json:"_components"`
	}
	require.NoError(t, json.Unmarshal(body, &document))
	for _, state := range document.Components {
		if state["stream"] != stream {
			continue
		}
		require.Equal(t, "recording", state["component"])
		require.Equal(t, float64(7), state["revision"])
		require.Equal(t, "failed", state["state"])
		require.Equal(t, float64(1), state["failures"])
		require.Positive(t, state["last_failure_at"])
		require.Len(t, state, 6, "internal ownership must not leak into the admin response")
		return
	}
	t.Fatal("runtime component missing from admin configuration response")
}
