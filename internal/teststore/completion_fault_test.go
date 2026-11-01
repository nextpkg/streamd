package teststore

import (
	"crypto/sha256"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCompletedPartEvidenceRejectsWeakIdentity(test *testing.T) {
	body := []byte("first-second")
	valid := map[int]PartDigest{1: {Bytes: 6, SHA256: fmt.Sprintf("%x", sha256.Sum256(body[:6]))}, 2: {Bytes: 6, SHA256: fmt.Sprintf("%x", sha256.Sum256(body[6:]))}}
	if err := VerifyCompletedParts(body, valid); err != nil {
		test.Fatal(err)
	}
	for _, scenario := range []string{"same-size-different-bytes", "missing", "gap", "negative", "too-large", "extra-bytes", "empty"} {
		test.Run(scenario, func(test *testing.T) {
			parts, data := maps.Clone(valid), append([]byte(nil), body...)
			switch scenario {
			case "same-size-different-bytes":
				data[0] ^= 1
			case "missing":
				delete(parts, 2)
			case "gap":
				parts[3] = parts[2]
				delete(parts, 2)
			case "negative":
				parts[1] = PartDigest{Bytes: -1}
			case "too-large":
				parts[1] = PartDigest{Bytes: 100}
			case "extra-bytes":
				data = append(data, 1)
			case "empty":
				parts = nil
			}
			if err := VerifyCompletedParts(data, parts); err == nil {
				test.Fatal("unproven completed object accepted")
			}
		})
	}
}

func TestCompletionRetryEvidenceRequiresStableUploadAndManifest(test *testing.T) {
	for _, scenario := range []string{"same", "changed-upload", "changed-manifest"} {
		test.Run(scenario, func(test *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				response.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()
			fault, err := NewCompletionFault(upstream.URL, "", "response-lost")
			if err != nil {
				test.Fatal(err)
			}
			defer fault.Close()
			client := &http.Client{Timeout: time.Second}
			response, err := client.Post(fault.URL+"/key?uploads", "application/xml", strings.NewReader("create"))
			if err != nil {
				test.Fatal(err)
			}
			_ = response.Body.Close()
			if err := fault.Arm([]string{"key"}); err != nil {
				test.Fatal(err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				identity, manifest := "same-upload", "same-manifest"
				if attempt == 1 && scenario == "changed-upload" {
					identity = "different-upload"
				}
				if attempt == 1 && scenario == "changed-manifest" {
					manifest = "different-manifest"
				}
				response, err := client.Post(fault.URL+"/key?uploadId="+identity, "application/xml", strings.NewReader(manifest))
				if err == nil {
					_ = response.Body.Close()
					test.Fatal("post-completion response was not dropped")
				}
			}
			outcomes, failures := fault.Snapshot()
			if outcomes["key"].SuccessfulCompletions != 2 || outcomes["key"].Dropped != 2 || (scenario == "same" && len(failures) != 0) || (scenario != "same" && len(failures) != 1) {
				test.Fatalf("wrong retry identity evidence: %+v %v", outcomes, failures)
			}
		})
	}
}
