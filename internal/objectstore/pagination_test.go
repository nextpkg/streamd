package objectstore

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type listingPage struct {
	XMLName              xml.Name
	IsTruncated          bool
	NextKeyMarker        string          `xml:",omitempty"`
	NextUploadIDMarker   string          `xml:"NextUploadIdMarker,omitempty"`
	NextPartNumberMarker string          `xml:",omitempty"`
	Uploads              []listingUpload `xml:"Upload,omitempty"`
	Parts                []listingPart   `xml:"Part,omitempty"`
}

type listingUpload struct {
	Key string
	ID  string `xml:"UploadId"`
}

type listingPart struct {
	Number int32 `xml:"PartNumber"`
	ETag   string
	Size   int64
}

func listingClient(test *testing.T, provider, endpoint string) *Client {
	test.Helper()
	var client *Client
	var err error
	if provider == "S3" {
		client, err = NewS3(&S3Config{Enable: true, Endpoint: endpoint, Bucket: "media", SecretID: "test", SecretKey: "test", PathStyle: true})
	} else {
		client, err = NewCOS(&COSConfig{Enable: true, Endpoint: endpoint, SecretID: "test", SecretKey: "test"})
	}
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(client.Close)
	return client
}

func listingResult(ctx context.Context, client *Client, operation string) (int, error) {
	if operation == "parts" {
		parts, err := client.Backend.Parts(ctx, "live/中文 +.ts", "upload-id")
		return len(parts), err
	}
	uploads, err := client.Backend.Uploads(ctx, "live/")
	return len(uploads), err
}

func listingResponse(writer http.ResponseWriter, operation string, page listingPage) {
	page.XMLName.Local = "ListMultipartUploadsResult"
	if operation == "parts" {
		page.XMLName.Local = "ListPartsResult"
	}
	writer.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(writer).Encode(page)
}

func TestObjectListingPreservesAllPagesAndOpaqueUploadIDs(test *testing.T) {
	for _, provider := range []string{"S3", "COS"} {
		for _, operation := range []string{"uploads", "parts"} {
			test.Run(provider+"/"+operation, func(test *testing.T) {
				keys := []string{"live/中文 +.ts", "live/中文 +.ts", "live/第二.ts"}
				ids := []string{"z-last-lexically", "a-first-lexically", "next-key"}
				var requests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					index := int(requests.Add(1)) - 1
					if index >= len(keys) {
						test.Error("unexpected extra page request")
						writer.WriteHeader(http.StatusForbidden)
						return
					}
					query := request.URL.Query()
					if request.Method != http.MethodGet || request.Header.Get("Authorization") == "" {
						test.Error("listing must use a signed GET")
					}
					if operation == "parts" {
						marker := ""
						if index > 0 {
							marker = fmt.Sprint(index)
						}
						objectPath := "/" + keys[0]
						if provider == "S3" {
							objectPath = "/media" + objectPath
						}
						if request.URL.Path != objectPath || query.Get("uploadId") != "upload-id" || query.Get("part-number-marker") != marker {
							test.Errorf("part request lost identity or marker: %s", request.URL)
						}
					} else {
						keyMarker, idMarker := "", ""
						if index > 0 {
							keyMarker, idMarker = keys[index-1], ids[index-1]
						}
						if !query.Has("uploads") || query.Get("prefix") != "live/" || query.Get("key-marker") != keyMarker || query.Get("upload-id-marker") != idMarker {
							test.Errorf("upload request lost prefix or compound marker: %s", request.URL)
						}
					}
					listingResponse(writer, operation, listingPage{IsTruncated: index < 2, NextKeyMarker: keys[index], NextUploadIDMarker: ids[index], NextPartNumberMarker: fmt.Sprint(index + 1), Uploads: []listingUpload{{keys[index], ids[index]}}, Parts: []listingPart{{int32(index + 1), fmt.Sprintf("opaque-etag-%d", index), 5 << 20}}})
				}))
				defer server.Close()
				client := listingClient(test, provider, server.URL)
				if operation == "parts" {
					actual, err := client.Backend.Parts(context.Background(), keys[0], "upload-id")
					wanted := []Part{{1, "opaque-etag-0", 5 << 20}, {2, "opaque-etag-1", 5 << 20}, {3, "opaque-etag-2", 5 << 20}}
					if err != nil || !reflect.DeepEqual(actual, wanted) {
						test.Fatalf("parts=%+v err=%v", actual, err)
					}
				} else {
					actual, err := client.Backend.Uploads(context.Background(), "live/")
					wanted := []Upload{{keys[0], ids[0]}, {keys[1], ids[1]}, {keys[2], ids[2]}}
					if err != nil || !reflect.DeepEqual(actual, wanted) {
						test.Fatalf("uploads=%+v err=%v", actual, err)
					}
				}
				if requests.Load() != 3 {
					test.Fatalf("requests=%d", requests.Load())
				}
			})
		}
	}
}

func TestObjectListingRejectsMissingAndCyclicMarkers(test *testing.T) {
	for _, provider := range []string{"S3", "COS"} {
		for _, operation := range []string{"uploads", "parts"} {
			for _, scenario := range []struct {
				name    string
				markers []string
			}{
				{"missing-first", []string{""}},
				{"missing-later", []string{"1", ""}},
				{"repeated", []string{"1", "1"}},
				{"cycle", []string{"1", "2", "1"}},
			} {
				test.Run(provider+"/"+operation+"/"+scenario.name, func(test *testing.T) {
					var requests atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						index := int(requests.Add(1)) - 1
						if index >= len(scenario.markers) {
							writer.WriteHeader(http.StatusForbidden)
							fmt.Fprint(writer, "<Error><Code>AccessDenied</Code></Error>")
							return
						}
						marker := scenario.markers[index]
						listingResponse(writer, operation, listingPage{IsTruncated: true, NextKeyMarker: marker, NextUploadIDMarker: marker, NextPartNumberMarker: marker, Uploads: []listingUpload{{"live/1.ts", "id"}}, Parts: []listingPart{{1, "etag", 5 << 20}}})
					}))
					defer server.Close()
					client := listingClient(test, provider, server.URL)
					count, err := listingResult(context.Background(), client, operation)
					if err == nil || !strings.Contains(err.Error(), "pagination") || count != 0 || requests.Load() != int32(len(scenario.markers)) {
						test.Fatalf("unsafe listing: count=%d requests=%d err=%v", count, requests.Load(), err)
					}
				})
			}
		}
	}
}

func TestObjectListingDiscardsPartialResultsOnErrorOrCancellation(test *testing.T) {
	for _, provider := range []string{"S3", "COS"} {
		for _, operation := range []string{"uploads", "parts"} {
			for _, cancelRequest := range []bool{false, true} {
				test.Run(fmt.Sprintf("%s/%s/cancel=%t", provider, operation, cancelRequest), func(test *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					var requests atomic.Int32
					cancelled := make(chan struct{})
					server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						if requests.Add(1) == 1 {
							listingResponse(writer, operation, listingPage{IsTruncated: true, NextKeyMarker: "live/1.ts", NextUploadIDMarker: "id", NextPartNumberMarker: "1", Uploads: []listingUpload{{"live/1.ts", "id"}}, Parts: []listingPart{{1, "etag", 5 << 20}}})
							return
						}
						if cancelRequest {
							cancel()
							<-request.Context().Done()
							close(cancelled)
							return
						}
						writer.WriteHeader(http.StatusForbidden)
						fmt.Fprint(writer, "<Error><Code>AccessDenied</Code></Error>")
					}))
					defer server.Close()
					client := listingClient(test, provider, server.URL)
					count, err := listingResult(ctx, client, operation)
					if err == nil || count != 0 || requests.Load() != 2 {
						test.Fatalf("partial listing escaped: count=%d requests=%d err=%v", count, requests.Load(), err)
					}
					if cancelRequest {
						if !errors.Is(err, context.Canceled) {
							test.Fatalf("cancellation lost: %v", err)
						}
						select {
						case <-cancelled:
						case <-time.After(time.Second):
							test.Fatal("server request was not cancelled")
						}
					} else if !strings.Contains(err.Error(), "AccessDenied") {
						test.Fatalf("permission error lost: %v", err)
					}
				})
			}
		}
	}
}

func TestObjectInspectionRejectsAmbiguityAcrossPagesWithoutMutation(test *testing.T) {
	for _, provider := range []string{"S3", "COS"} {
		test.Run(provider, func(test *testing.T) {
			var requests, mutations atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodGet || !request.URL.Query().Has("uploads") {
					mutations.Add(1)
					writer.WriteHeader(http.StatusForbidden)
					return
				}
				index := requests.Add(1)
				listingResponse(writer, "uploads", listingPage{IsTruncated: index == 1, NextKeyMarker: "live/1.ts", NextUploadIDMarker: "first", Uploads: []listingUpload{{"live/1.ts", fmt.Sprint(index)}}})
			}))
			defer server.Close()
			client := listingClient(test, provider, server.URL)
			result, err := client.InspectExpired(context.Background(), "live/current.ts", 900, func(string, int64) bool { return true })
			if err == nil || !strings.Contains(err.Error(), "ambiguous") || result.Finalized != 0 || requests.Load() != 2 || mutations.Load() != 0 {
				test.Fatalf("ambiguous upload changed: result=%+v requests=%d other=%d err=%v", result, requests.Load(), mutations.Load(), err)
			}
		})
	}
}

func TestObjectInspectionCloseCancelsSecondPageHTTP(test *testing.T) {
	for _, provider := range []string{"S3", "COS"} {
		test.Run(provider, func(test *testing.T) {
			entered, cancelled := make(chan struct{}), make(chan struct{})
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if requests.Add(1) == 1 {
					listingResponse(writer, "uploads", listingPage{IsTruncated: true, NextKeyMarker: "live/1.ts", NextUploadIDMarker: "id", Uploads: []listingUpload{{"live/1.ts", "id"}}})
					return
				}
				close(entered)
				<-request.Context().Done()
				close(cancelled)
			}))
			test.Cleanup(server.Close)
			client := listingClient(test, provider, server.URL)
			client.inspectionDelay = func() time.Duration { return 0 }
			if !client.ScheduleInspection("live/current.ts", 900, func(string, int64) bool { return true }) {
				test.Fatal("inspection not scheduled")
			}
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				test.Fatal("second page did not start")
			}
			closed := make(chan struct{})
			go func() { client.Close(); close(closed) }()
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				test.Fatal("Close did not join cancelled listing")
			}
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				test.Fatal("second-page socket still active")
			}
			stats := client.InspectionStats()
			if stats.Cancelled != 1 || stats.Pending != 0 || stats.Running != 0 || stats.Finalized != 0 || requests.Load() != 2 {
				test.Fatalf("retirement mismatch: %+v requests=%d", stats, requests.Load())
			}
		})
	}
}
