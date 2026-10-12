package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStabilityRejectsMediaErrorLogs(test *testing.T) {
	filename := filepath.Join(test.TempDir(), "play-hls.log")
	if err := os.WriteFile(filename, nil, 0600); err != nil {
		test.Fatal(err)
	}
	if err := requireSilentMediaLog(filename); err != nil {
		test.Fatal(err)
	}
	for _, text := range []string{"Error reading HTTP response: End of file\n", strings.Repeat("unexpected error\n", 10000)} {
		if err := os.WriteFile(filename, []byte(text), 0600); err != nil {
			test.Fatal(err)
		}
		if err := requireSilentMediaLog(filename); err == nil || !strings.Contains(err.Error(), "play-hls.log") || len(err.Error()) > 4200 {
			test.Fatalf("missing or unbounded early media failure: %v", err)
		}
	}
	if err := os.Remove(filename); err != nil {
		test.Fatal(err)
	}
	if requireSilentMediaLog(filename) == nil {
		test.Fatal("missing media log accepted")
	}
}

func TestStabilityProgressReadsOnlyCompleteAppendedFrames(test *testing.T) {
	filename := filepath.Join(test.TempDir(), "progress.txt")
	progress := &stabilityProgress{}
	if frames, err := progress.read(filename); err != nil || frames != 0 {
		test.Fatalf("missing initial progress: %d %v", frames, err)
	}
	file, err := os.Create(filename)
	if err != nil {
		test.Fatal(err)
	}
	defer file.Close()
	for _, step := range []struct {
		body   string
		frames int
		ended  bool
	}{{"frame=1", 0, false}, {"2\nprogress=continue\nframe=", 12, false}, {"25\nprogress=end\n", 25, true}, {"", 25, true}} {
		if _, err := file.WriteString(step.body); err != nil {
			test.Fatal(err)
		}
		frames, err := progress.read(filename)
		if err != nil || frames != step.frames || progress.ended != step.ended {
			test.Fatalf("appended progress: frames=%d end=%v error=%v", frames, progress.ended, err)
		}
	}
	if progress.pending != "" {
		test.Fatal("completed progress retained a partial line")
	}
}

func TestStabilityProgressRejectsResetAndUnboundedInput(test *testing.T) {
	for _, mode := range []string{"truncate", "decrease", "negative", "invalid", "chunk", "line"} {
		test.Run(mode, func(test *testing.T) {
			filename := filepath.Join(test.TempDir(), "progress.txt")
			if err := os.WriteFile(filename, []byte("frame=25\n"), 0600); err != nil {
				test.Fatal(err)
			}
			progress := &stabilityProgress{}
			if _, err := progress.read(filename); err != nil {
				test.Fatal(err)
			}
			body := map[string]string{"truncate": "", "decrease": "frame=24\nframe=26\n", "negative": "frame=-1\n", "invalid": "frame=no\n", "chunk": strings.Repeat("x", 65537), "line": strings.Repeat("x", 4097)}[mode]
			if mode != "truncate" {
				body = "frame=25\n" + body
			}
			if err := os.WriteFile(filename, []byte(body), 0600); err != nil {
				test.Fatal(err)
			}
			if _, err := progress.read(filename); err == nil {
				test.Fatal("accepted corrupt or unbounded progress")
			}
		})
	}
}
