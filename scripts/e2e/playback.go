package main

import (
	"fmt"
	"maps"
	"time"
)

// playersForApps starts all three protocol decoders for both recording formats.
// Each scenario owns its watch and timing; harness.cleanup owns child processes,
// including players already launched when a later app fails to become ready.
func (harness *harness) playersForApps(seconds int) (map[string]*process, error) {
	players := make(map[string]*process)
	for _, app := range []string{"flv", "ts"} {
		if _, err := harness.waitPlaylist(app); err != nil {
			return nil, fmt.Errorf("%s playback readiness: %w", app, err)
		}
		group, err := harness.players(app, app+"-", seconds)
		if err != nil {
			return nil, fmt.Errorf("%s playback start: %w", app, err)
		}
		maps.Copy(players, group)
	}
	return players, nil
}

// finishPublishers waits for natural completion; scenarios choose when to call it
// and retain ownership of server shutdown and post-publication assertions.
func (harness *harness) finishPublishers(publishers map[string]*process, prefix string, timeout time.Duration) error {
	for app, child := range publishers {
		if err := harness.finish(prefix+app, child, timeout); err != nil {
			return err
		}
	}
	return nil
}
