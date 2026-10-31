package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Core covers normal media, configuration changes, republishing and recording
// resume on the local filesystem. Infrastructure and fault scenarios stay opt-in.
var suites = map[string][]string{
	"core": {"stream", "reload", "republish", "resume"},
}

type scenarioResult struct {
	Name     string  `json:"scenario"`
	Status   string  `json:"status"`
	Evidence string  `json:"evidence,omitempty"`
	Failure  string  `json:"failure,omitempty"`
	Seconds  float64 `json:"seconds"`
}

type suiteResult struct {
	Suite     string           `json:"suite"`
	Complete  bool             `json:"complete"`
	Scenarios []scenarioResult `json:"scenarios"`
}

type scenarioRunner func(context.Context, string, string, bool) (string, error)

// Run serially: scenario fault schedules remain independent, and a failure never
// disappears behind a later successful scenario. Unstarted cases stay explicit.
func executeSuite(ctx context.Context, root, suite string, race bool, run scenarioRunner) error {
	names, ok := suites[suite]
	if !ok {
		return fmt.Errorf("unsupported suite %q", suite)
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(root, "artifacts"), 0700); err != nil {
		return err
	}
	directory, err := os.MkdirTemp(filepath.Join(root, "artifacts"), "suite-"+suite+"-")
	if err != nil {
		return err
	}
	filename := filepath.Join(directory, "results.json")
	fmt.Println(filename)
	result := suiteResult{Suite: suite}
	for _, name := range names {
		result.Scenarios = append(result.Scenarios, scenarioResult{Name: name, Status: "not_run"})
	}
	for index, name := range names {
		if err := ctx.Err(); err != nil {
			result.Scenarios[index].Status = "cancelled"
			result.Scenarios[index].Failure = err.Error()
			if writeErr := writeJSON(filename, result); writeErr != nil {
				return errors.Join(err, fmt.Errorf("save suite result: %w", writeErr))
			}
			return err
		}
		entry := &result.Scenarios[index]
		entry.Status = "running"
		if err := writeJSON(filename, result); err != nil {
			return err
		}
		started := time.Now()
		entry.Evidence, err = run(ctx, root, name, race)
		entry.Seconds = time.Since(started).Seconds()
		entry.Status = "passed"
		if err != nil {
			entry.Status = "failed"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				entry.Status = "cancelled"
			}
			entry.Failure = err.Error()
		}
		result.Complete = err == nil && index == len(names)-1
		if writeErr := writeJSON(filename, result); writeErr != nil {
			return errors.Join(err, fmt.Errorf("save suite result: %w", writeErr))
		}
		if err != nil {
			return fmt.Errorf("scenario %s: %w", name, err)
		}
	}
	return nil
}
