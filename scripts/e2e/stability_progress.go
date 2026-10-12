package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func requireSilentMediaLog(filename string) error {
	info, err := os.Stat(filename)
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil
	}
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, 4096))
	if err != nil {
		return err
	}
	return fmt.Errorf("unexpected media error in %s: %s", filepath.Base(filename), body)
}

type stabilityProgress struct {
	offset  int64
	pending string
	frames  int
	ended   bool
}

func (progress *stabilityProgress) read(filename string) (int, error) {
	file, err := os.Open(filename)
	if errors.Is(err, os.ErrNotExist) && progress.offset == 0 {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() < progress.offset {
		return 0, errors.New("progress file was truncated")
	}
	if _, err := file.Seek(progress.offset, io.SeekStart); err != nil {
		return 0, err
	}
	body, err := io.ReadAll(io.LimitReader(file, 65537))
	if err != nil {
		return 0, err
	}
	if len(body) > 65536 {
		return 0, errors.New("progress chunk exceeds 64 KiB")
	}
	progress.offset += int64(len(body))
	text := progress.pending + string(body)
	boundary := strings.LastIndexByte(text, '\n')
	progress.pending = text[boundary+1:]
	if len(progress.pending) > 4096 {
		return 0, errors.New("incomplete progress line exceeds 4 KiB")
	}
	if boundary < 0 {
		return progress.frames, nil
	}
	for _, line := range strings.Split(text[:boundary], "\n") {
		if line == "progress=end" {
			progress.ended = true
		}
		value, present := strings.CutPrefix(line, "frame=")
		if !present {
			continue
		}
		frames, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || frames < progress.frames {
			return 0, fmt.Errorf("invalid or decreasing decoded frame count %q", value)
		}
		progress.frames = frames
	}
	return progress.frames, nil
}
