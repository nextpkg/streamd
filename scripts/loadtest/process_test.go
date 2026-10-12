package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMain(runner *testing.M) {
	mode := os.Getenv("STREAMD_LOADTEST_HELPER")
	if mode == "" {
		os.Exit(runner.Run())
	}
	if mode == "exit" {
		os.Exit(7)
	}
	signals := make(chan os.Signal, 1)
	if mode == "ignore" {
		signal.Ignore(syscall.SIGTERM)
	} else {
		signal.Notify(signals, syscall.SIGTERM)
	}
	if err := os.WriteFile(os.Getenv("STREAMD_LOADTEST_READY"), []byte("ready"), 0600); err != nil {
		os.Exit(8)
	}
	for {
		select {
		case <-signals:
			os.Exit(0)
		case <-time.After(time.Hour):
		}
	}
}

func helperChild(test *testing.T, mode string) *child {
	directory := test.TempDir()
	ready := filepath.Join(directory, "ready")
	environment := append(os.Environ(), "STREAMD_LOADTEST_HELPER="+mode, "STREAMD_LOADTEST_READY="+ready)
	process, err := startChild(os.Args[0], []string{"-test.run=^$"}, environment, io.Discard, filepath.Join(directory, "child.log"))
	require.NoError(test, err)
	test.Cleanup(func() { _ = process.stop(time.Second) })
	if mode != "exit" {
		require.Eventually(test, func() bool { _, err := os.Stat(ready); return err == nil }, 5*time.Second, 5*time.Millisecond)
	}
	return process
}

func TestLoadChildCancellationJoinsOnlyOwnedProcess(test *testing.T) {
	sentinel := helperChild(test, "wait")
	target := helperChild(test, "wait")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(test, target.wait(ctx), context.Canceled)
	select {
	case <-target.done:
	default:
		test.Fatal("cancelled child was not joined")
	}
	select {
	case <-sentinel.done:
		test.Fatal("unrelated process was stopped")
	default:
	}
	require.NoError(test, sentinel.stop(2*time.Second))
}

func TestLoadChildEscalatesAndJoinsUnresponsiveProcess(test *testing.T) {
	process := helperChild(test, "ignore")
	require.ErrorContains(test, process.stop(20*time.Millisecond), "shutdown grace")
	select {
	case <-process.done:
	default:
		test.Fatal("killed child was not joined")
	}
}

func TestLoadChildExitAndLogOwnershipArePreserved(test *testing.T) {
	process := helperChild(test, "exit")
	err := process.wait(context.Background())
	var exit *exec.ExitError
	require.True(test, errors.As(err, &exit))
	require.Equal(test, 7, exit.ExitCode())
	filename := filepath.Join(test.TempDir(), "existing.log")
	require.NoError(test, os.WriteFile(filename, []byte("keep"), 0600))
	_, err = startChild(os.Args[0], nil, nil, io.Discard, filename)
	require.ErrorIs(test, err, os.ErrExist)
	body, err := os.ReadFile(filename)
	require.NoError(test, err)
	require.Equal(test, "keep", string(body))
}
