package objectstore

import (
	"bytes"
	"context"
	"testing"
)

func TestExpiredInspectionRetryOverwritesProvisionalTail(test *testing.T) {
	saved := validCheckpoint()
	client, backend := manifestInspectionClient(test, saved)
	backend.parts = append(backend.parts, Part{Number: 2, ETag: "untrusted-provisional-tail", Size: int64(len(saved.Tail))})
	result, err := client.InspectExpired(context.Background(), saved.Key, 900, func(string, int64) bool { return true })
	if err != nil || result.Finalized != 1 || backend.lists != 1 || backend.partLists != 1 || backend.completes != 1 || backend.aborts != 0 || !bytes.Equal(backend.tail, saved.Tail) || len(backend.manifest) != 2 || backend.manifest[1].ETag != "tail-part" || backend.saved != nil {
		test.Fatalf("provisional tail was trusted, duplicated or lost: result=%+v error=%v backend=%+v", result, err, backend)
	}
}

func TestExpiredInspectionRetryRejectsUnprovenManifests(test *testing.T) {
	for _, scenario := range []struct {
		name   string
		modify func(*inspectionManifestBackend)
	}{
		{"changed-prefix", func(backend *inspectionManifestBackend) { backend.parts[0].ETag = "changed" }},
		{"wrong-tail-size", func(backend *inspectionManifestBackend) { backend.parts[1].Size++ }},
		{"part-gap", func(backend *inspectionManifestBackend) { backend.parts[1].Number++ }},
		{"missing-tail-etag", func(backend *inspectionManifestBackend) { backend.parts[1].ETag = "" }},
		{"new-full-part", func(backend *inspectionManifestBackend) { backend.parts[1].Size = MinBlockSize }},
		{"multiple-extra-parts", func(backend *inspectionManifestBackend) {
			backend.parts = append(backend.parts, Part{Number: 3, ETag: "extra", Size: 2})
		}},
		{"wrong-upload", func(backend *inspectionManifestBackend) { backend.uploads[0].ID = "replacement" }},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			saved := validCheckpoint()
			client, backend := manifestInspectionClient(test, saved)
			backend.parts = append(backend.parts, Part{Number: 2, ETag: "untrusted", Size: int64(len(saved.Tail))})
			scenario.modify(backend)
			checkpoint := bytes.Clone(backend.saved)
			result, err := client.InspectExpired(context.Background(), saved.Key, 900, func(string, int64) bool { return true })
			if err == nil || result.Finalized != 0 || backend.completes != 0 || backend.aborts != 0 || backend.tail != nil || !bytes.Equal(backend.saved, checkpoint) {
				test.Fatalf("unproven upload modified: result=%+v error=%v backend=%+v", result, err, backend)
			}
		})
	}
}
