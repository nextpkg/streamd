package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nextpkg/streamd/rtmp/core"
)

func (harness *harness) rejectStreamPaths(stage string) error {
	cases := [][2]string{}
	for _, name := range []string{"", ".", "..", "../ts/sample", "camera/../../ts/sample", "./sample", "/sample", "sample/", "camera//front", "camera\\front", "sample\x00", "sample\n", "sample\x7f", "\xff"} {
		cases = append(cases, [2]string{"flv", name})
	}
	for _, app := range []string{"", ".", "..", "flv/other", "flv\\other", "flv\x00", "flv\n", "flv\x7f", "\xff"} {
		cases = append(cases, [2]string{app, "sample"})
	}
	checks := []map[string]any{}
	for _, identity := range cases {
		for _, method := range []string{core.Publish, core.Play} {
			started := time.Now()
			if err := rejectStreamPath(harness.rtmpPort, identity[0], identity[1], method); err != nil {
				return err
			}
			checks = append(checks, map[string]any{"app_bytes": fmt.Sprintf("%x", identity[0]), "instance_bytes": fmt.Sprintf("%x", identity[1]), "method": method, "closed_before_success": true, "seconds": time.Since(started).Seconds()})
		}
	}
	keys, err := harness.adminData(harness.ctx, "/admin/stat/keys")
	if err != nil {
		return err
	}
	if keys["count"] != float64(2) {
		return fmt.Errorf("%s unsafe names changed stream count: %v", stage, keys)
	}
	stages, _ := harness.results["stream_path_rejections"].(map[string]any)
	if stages == nil {
		stages = map[string]any{}
		harness.results["stream_path_rejections"] = stages
	}
	stages[stage] = map[string]any{"checks": checks, "stream_count": 2}
	return nil
}

func rejectStreamPath(port int, app, instance, method string) error {
	transport, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = transport.Close() }()
	if err := transport.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	client := core.NewConnClient(app, instance, core.NewConn(transport, core.ConnBufSize))
	err = client.Start(method)
	if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		return fmt.Errorf("unsafe %s %q/%q received success or did not close promptly: %v", method, app, instance, err)
	}
	return nil
}

func (harness *harness) recordingPathsUnaliased() error {
	root := filepath.Join(harness.evidence, "recordings")
	err := filepath.WalkDir(root, func(filename string, entry fs.DirEntry, walkError error) error {
		if walkError != nil {
			return walkError
		}
		relative, err := filepath.Rel(root, filename)
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			relative = filepath.Dir(relative)
		}
		switch filepath.ToSlash(relative) {
		case ".", "flv", "ts", "flv/sample", "ts/sample":
			return nil
		default:
			return fmt.Errorf("unexpected recording path after rejected requests: %q", relative)
		}
	})
	if err == nil {
		harness.results["recording_paths_unaliased"] = true
	}
	return err
}
