package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	scenario := flag.String("scenario", "", "single scenario (default stream); use -list for all names")
	suite := flag.String("suite", "", "run a named suite: core")
	list := flag.Bool("list", false, "list scenarios and suites without starting services")
	root := flag.String("root", ".", "streamd module directory")
	race := flag.Bool("race", os.Getenv("STREAMD_E2E_RACE") == "1", "build the service with the race detector")
	inspect := flag.String("inspect-admin-recordings", "", "verify saved admin-scenario recordings against independent source references without starting a service")
	flag.Parse()
	if *list {
		if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"scenarios": scenarioNames(), "suites": suites}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if (*suite != "" && *scenario != "") || (*inspect != "" && (*suite != "" || *scenario != "")) {
		fmt.Fprintln(os.Stderr, "choose only one of -scenario, -suite, or -inspect-admin-recordings")
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *inspect != "" {
		harness := &harness{ctx: ctx, root: *root, evidence: *inspect, results: map[string]any{}}
		if err := harness.takeoverRecordingIdentity(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := json.NewEncoder(os.Stdout).Encode(harness.results); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	var err error
	if *suite != "" {
		err = executeSuite(ctx, *root, *suite, *race, execute)
	} else {
		if *scenario == "" {
			*scenario = "stream"
		}
		_, err = execute(ctx, *root, *scenario, *race)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func execute(ctx context.Context, root, scenario string, race bool) (evidence string, result error) {
	spec, exists := scenarios[scenario]
	if !exists {
		return "", fmt.Errorf("unsupported scenario %q", scenario)
	}
	if scenario == "stream" {
		if _, err := streamDuration(os.Getenv("STREAMD_E2E_SECONDS")); err != nil {
			return "", err
		}
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return "", fmt.Errorf("run from the streamd module or provide -root: %w", err)
	}
	harness, err := newHarness(ctx, root, spec.Prefix, spec.Marker, race)
	if err != nil {
		return "", err
	}
	evidence = harness.evidence
	defer func() {
		harness.cleanup()
		if result != nil {
			harness.results["failure"] = result.Error()
		}
		harness.results["complete"] = result == nil
		result = errors.Join(result, writeJSON(filepath.Join(harness.evidence, "results.json"), harness.results))
		fmt.Println(harness.evidence)
	}()
	harness.results["scenario"] = scenario
	return evidence, spec.Run(harness)
}
