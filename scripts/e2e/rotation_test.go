package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
)

func TestRotationCompletionProxyPreservesSigningAuthority(test *testing.T) {
	body := []byte("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>")
	backend := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		actual, err := io.ReadAll(request.Body)
		if err != nil || request.Host != "signed-authority.invalid:9000" || request.URL.RequestURI() != "/flv%2Fsample%2F1.flv?uploadId=upload-1" || request.Header.Get("Authorization") != "signed-request" || !bytes.Equal(actual, body) {
			response.WriteHeader(http.StatusForbidden)
			return
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()
	request := httptest.NewRequest(http.MethodPost, "http://signed-authority.invalid:9000/flv%2Fsample%2F1.flv?uploadId=upload-1", bytes.NewReader(body))
	request.Header.Set("Authorization", "signed-request")
	response, err := forwardRotationCompletion(backend.Client(), backend.URL, request, body)
	if err != nil {
		test.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		test.Fatal("completion proxy changed the signed authority, path, query, headers or manifest")
	}
}

func TestRotationCallbacksRequireExactCompletedInventory(test *testing.T) {
	for _, scenario := range []string{"valid", "missing-completion", "unknown-file", "duplicate-file", "wrong-generation", "missing-publisher"} {
		test.Run(scenario, func(test *testing.T) {
			files := []string{"initial/flv/sample/1720000000.flv", "recovered/ts/sample/1720000020.ts"}
			observations := []callbackObservation{}
			for _, app := range []string{"flv", "ts"} {
				for _, event := range []string{"OnPublish", "OnPublishDone"} {
					observations = append(observations, callbackObservation{Path: "/publication", Status: 200, Received: 1720000060000, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1720000000000)}})
				}
			}
			for _, filename := range files {
				generation, relative, _ := strings.Cut(filename, "/")
				app := strings.Split(relative, "/")[0]
				for index, event := range []string{"OnRecord", "OnRecordDone"} {
					observations = append(observations, callbackObservation{Path: "/" + generation, Status: 200, Received: 1720000060000, Payload: map[string]any{"event": event, "key": app + "/sample", "occur": float64(1720000000000 + index*1000), "location": "/" + relative, "filename": path.Base(relative), "connInfo": map[string]any{"app": app}}})
				}
			}
			switch scenario {
			case "missing-completion":
				observations = observations[:len(observations)-1]
			case "unknown-file":
				files = append(files, "initial/flv/sample/1720000004.flv")
			case "duplicate-file":
				files = append(files, files[0])
			case "wrong-generation":
				observations[5].Path = "/recovered"
			case "missing-publisher":
				observations = observations[1:]
			}
			_, err := validateStorageCallbacks(observations, files, nil, 1720000000000)
			if (err == nil) != (scenario == "valid") {
				test.Fatalf("unexpected rotation callback decision: %v", err)
			}
			if scenario == "valid" {
				if _, err := validateStorageFaultCallbacks(observations, files, "", 1720000000000); err == nil {
					test.Fatal("fault wrapper accepted missing failed-object evidence")
				}
			}
		})
	}
}
