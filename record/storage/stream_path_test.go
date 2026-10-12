package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
)

func TestStorageRejectsAliasedStreamPaths(test *testing.T) {
	for _, info := range []*base.Info{
		base.NewInfo("live", "../other/sample", false),
		base.NewInfo("live", "camera/../../other/sample", false),
		base.NewInfo("live", "./sample", false),
		base.NewInfo("live", "sample/", false),
		base.NewInfo("live", "", false),
		base.NewInfo("..", "sample", false),
		base.NewInfo("live", "camera\\sample", false),
		{App: "live", Instance: "sample", Key: "other/sample"},
	} {
		test.Run(info.Key, func(test *testing.T) {
			directory := test.TempDir()
			settings := &config.AppRecorder{SaveTo: directory, SaveAs: "flv", SaveMode: "immediate", Driver: "fs", Segment: 4}
			store := NewStorage(info, settings, NewEvent())
			defer store.Release()
			err := store.Open()
			if err == nil {
				_ = store.Close(false)
				test.Errorf("invalid identity opened recording at %q", store.fullPath)
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 0 {
				test.Errorf("rejected identity changed recording directory: entries=%v error=%v", entries, err)
			}
		})
	}
}

func TestStoragePreservesLiteralStreamPaths(test *testing.T) {
	for _, name := range []string{"sample", "camera/front", "摄像头/大厅", "%2e%2e/sample", "clip.v1", "mp4:clip", "camera one"} {
		test.Run(name, func(test *testing.T) {
			directory := test.TempDir()
			settings := &config.AppRecorder{SaveTo: directory, SaveAs: "flv", SaveMode: "immediate", Driver: "fs", Segment: 4}
			store := NewStorage(base.NewInfo("live", name, false), settings, NewEvent())
			defer store.Release()
			if err := store.Open(); err != nil {
				test.Fatal(err)
			}
			if err := store.Write([]byte("literal stream name")); err != nil {
				test.Error(err)
			}
			if err := store.Close(false); err != nil {
				test.Fatal(err)
			}
			if filepath.Dir(store.fullPath) != filepath.Join(directory, "live", name) {
				test.Fatalf("stream name was rewritten: %q", store.fullPath)
			}
			contents, err := os.ReadFile(store.fullPath)
			if err != nil || string(contents) != "literal stream name" {
				test.Fatalf("recorded payload mismatch: %q %v", contents, err)
			}
		})
	}
}
