package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type child struct {
	command *exec.Cmd
	done    chan struct{}
	err     error
}

func startChild(executable string, arguments, environment []string, output io.Writer, logPath string) (*child, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	command := exec.Command(executable, arguments...)
	command.Env, command.Stdout, command.Stderr = environment, output, logFile
	command.WaitDelay = 2 * time.Second
	if err := command.Start(); err != nil {
		return nil, errors.Join(err, logFile.Close())
	}
	process := &child{command: command, done: make(chan struct{})}
	go func() {
		process.err = errors.Join(command.Wait(), logFile.Close())
		close(process.done)
	}()
	return process, nil
}

func (process *child) stop(grace time.Duration) error {
	select {
	case <-process.done:
		return process.err
	default:
	}
	_ = process.command.Process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-process.done:
		return process.err
	case <-timer.C:
		_ = process.command.Process.Kill()
		<-process.done
		return errors.Join(errors.New("child exceeded its shutdown grace period"), process.err)
	}
}

func (process *child) wait(ctx context.Context) error {
	select {
	case <-process.done:
		return process.err
	case <-ctx.Done():
		return errors.Join(ctx.Err(), process.stop(5*time.Second))
	}
}

type mediaProgress struct {
	Frames       int64 `json:"frames"`
	Microseconds int64 `json:"media_microseconds"`
	End          bool  `json:"end"`
}

type progressWriter struct {
	lock     sync.Mutex
	pending  []byte
	progress mediaProgress
}

func (writer *progressWriter) Write(data []byte) (int, error) {
	writer.lock.Lock()
	defer writer.lock.Unlock()
	length := len(data)
	for len(data) != 0 {
		lineEnd := bytes.IndexByte(data, '\n')
		if lineEnd < 0 {
			lineEnd = len(data)
		}
		if len(writer.pending)+lineEnd > 4096 {
			return 0, errors.New("FFmpeg progress line exceeds 4096 bytes")
		}
		writer.pending = append(writer.pending, data[:lineEnd]...)
		data = data[lineEnd:]
		if len(data) == 0 {
			break
		}
		data = data[1:]
		key, value, found := strings.Cut(strings.TrimSpace(string(writer.pending)), "=")
		writer.pending = writer.pending[:0]
		if !found {
			continue
		}
		switch key {
		case "frame", "out_time_us":
			parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err != nil {
				if value == "N/A" {
					continue
				}
				return 0, fmt.Errorf("invalid FFmpeg progress %s: %w", key, err)
			}
			if key == "frame" {
				writer.progress.Frames = parsed
			} else {
				writer.progress.Microseconds = parsed
			}
		case "progress":
			writer.progress.End = value == "end"
		}
	}
	return length, nil
}

func (writer *progressWriter) snapshot() mediaProgress {
	writer.lock.Lock()
	defer writer.lock.Unlock()
	return writer.progress
}
