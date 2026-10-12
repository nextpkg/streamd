package teststore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash/crc64"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type Part struct {
	Number int `xml:"PartNumber"`
	ETag   string
	Size   int
	Data   []byte `xml:"-"`
}
type upload struct {
	Key   string
	ID    string       `xml:"UploadId"`
	Parts map[int]Part `xml:"-"`
}
type Server struct {
	*httptest.Server
	mu      sync.Mutex
	bucket  string
	next    int
	uploads map[string]*upload
	objects map[string][]byte
	faults  map[string]int
	test    testing.TB
	errors  []string
}

func New(test testing.TB, bucket string) *Server {
	server := NewHTTP(bucket)
	server.test = test
	test.Cleanup(server.Close)
	return server
}

func NewHTTP(bucket string) *Server {
	server := &Server{bucket: bucket, uploads: make(map[string]*upload), objects: make(map[string][]byte), faults: make(map[string]int)}
	server.Server = httptest.NewServer(http.HandlerFunc(server.handle))
	return server
}

func (server *Server) Errors() []string {
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]string(nil), server.errors...)
}

func (server *Server) ObjectKeys() []string {
	server.mu.Lock()
	defer server.mu.Unlock()
	keys := make([]string, 0, len(server.objects))
	for key := range server.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (server *Server) PendingUploads() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return len(server.uploads)
}

func (server *Server) Reject(method, key string, status int) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.faults[method+" "+key] = status
}

func (server *Server) Store(key string, data []byte) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.objects[key] = bytes.Clone(data)
}

func (server *Server) Download(key string) []byte {
	server.test.Helper()
	prefix := ""
	if server.bucket != "" {
		prefix = "/" + server.bucket
	}
	response, err := http.Get(server.URL + prefix + "/" + key)
	if err != nil {
		server.test.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK {
		server.test.Fatalf("download failed: %v, HTTP %d", err, response.StatusCode)
	}
	return data
}

func writeXML(response http.ResponseWriter, value any) {
	response.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(response).Encode(value)
}

func (server *Server) handle(response http.ResponseWriter, request *http.Request) {
	server.mu.Lock()
	defer server.mu.Unlock()
	key := strings.TrimPrefix(request.URL.Path, "/")
	if server.bucket != "" {
		key = strings.TrimPrefix(strings.TrimPrefix(key, server.bucket), "/")
	}
	query := request.URL.Query()
	if request.Method != http.MethodGet && request.Header.Get("Authorization") == "" {
		server.errors = append(server.errors, "SDK request lacks authorization signature")
		if server.test != nil {
			server.test.Error("SDK request lacks authorization signature")
		}
	}
	if status := server.faults[request.Method+" "+key]; status != 0 {
		response.Header().Set("Content-Type", "application/xml")
		response.WriteHeader(status)
		_, _ = io.WriteString(response, "<Error><Code>AccessDenied</Code></Error>")
		return
	}
	if request.Method == http.MethodPost && query.Has("uploads") {
		server.next++
		id := strconv.Itoa(server.next)
		server.uploads[id] = &upload{Key: key, ID: id, Parts: make(map[int]Part)}
		writeXML(response, struct {
			XMLName xml.Name `xml:"InitiateMultipartUploadResult"`
			Key     string
			ID      string `xml:"UploadId"`
		}{Key: key, ID: id})
		return
	}
	if request.Method == http.MethodGet && query.Has("uploads") {
		result := struct {
			XMLName     xml.Name `xml:"ListMultipartUploadsResult"`
			IsTruncated bool
			Uploads     []upload `xml:"Upload"`
		}{}
		for _, item := range server.uploads {
			if strings.HasPrefix(item.Key, query.Get("prefix")) {
				result.Uploads = append(result.Uploads, *item)
			}
		}
		writeXML(response, result)
		return
	}
	if id := query.Get("uploadId"); id != "" {
		item := server.uploads[id]
		if item == nil || item.Key != key {
			http.Error(response, `<Error><Code>NoSuchUpload</Code></Error>`, http.StatusNotFound)
			return
		}
		switch request.Method {
		case http.MethodPut:
			number, err := strconv.Atoi(query.Get("partNumber"))
			if err != nil || number < 1 {
				http.Error(response, "invalid part", http.StatusBadRequest)
				return
			}
			data, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(response, "read body", http.StatusBadRequest)
				return
			}
			digest := sha256.Sum256(data)
			etag := `"` + hex.EncodeToString(digest[:]) + `"`
			item.Parts[number] = Part{Number: number, ETag: etag, Size: len(data), Data: data}
			response.Header().Set("ETag", etag)
			response.Header().Set("x-cos-hash-crc64ecma", strconv.FormatUint(crc64.Checksum(data, crc64.MakeTable(crc64.ECMA)), 10))
			response.WriteHeader(http.StatusOK)
		case http.MethodGet:
			result := struct {
				XMLName     xml.Name `xml:"ListPartsResult"`
				IsTruncated bool
				Parts       []Part `xml:"Part"`
			}{}
			for _, part := range item.Parts {
				result.Parts = append(result.Parts, part)
			}
			sort.Slice(result.Parts, func(left, right int) bool { return result.Parts[left].Number < result.Parts[right].Number })
			writeXML(response, result)
		case http.MethodPost:
			var completed struct {
				Parts []Part `xml:"Part"`
			}
			if err := xml.NewDecoder(request.Body).Decode(&completed); err != nil {
				http.Error(response, "invalid completion", http.StatusBadRequest)
				return
			}
			var object bytes.Buffer
			for index, wanted := range completed.Parts {
				part, ok := item.Parts[wanted.Number]
				if !ok || part.ETag != wanted.ETag || (index < len(completed.Parts)-1 && part.Size < 5*1024*1024) {
					http.Error(response, "invalid part manifest", http.StatusBadRequest)
					return
				}
				object.Write(part.Data)
			}
			server.objects[key] = bytes.Clone(object.Bytes())
			delete(server.uploads, id)
			writeXML(response, struct {
				XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
				Key     string
				ETag    string
			}{Key: key, ETag: `"completed"`})
		case http.MethodDelete:
			delete(server.uploads, id)
			response.WriteHeader(http.StatusNoContent)
		default:
			http.Error(response, "unsupported", http.StatusMethodNotAllowed)
		}
		return
	}
	if request.Method == http.MethodGet {
		data, ok := server.objects[key]
		if !ok {
			response.Header().Set("Content-Type", "application/xml")
			response.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(response, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		_, _ = response.Write(data)
		return
	}
	if request.Method == http.MethodPut {
		data, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(response, "read body", http.StatusBadRequest)
			return
		}
		server.objects[key] = bytes.Clone(data)
		digest := sha256.Sum256(data)
		response.Header().Set("ETag", `"`+hex.EncodeToString(digest[:])+`"`)
		response.Header().Set("x-cos-hash-crc64ecma", strconv.FormatUint(crc64.Checksum(data, crc64.MakeTable(crc64.ECMA)), 10))
		response.WriteHeader(http.StatusOK)
		return
	}
	if request.Method == http.MethodDelete {
		delete(server.objects, key)
		response.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(response, fmt.Sprintf("unsupported request %s %s", request.Method, request.URL.Path), http.StatusBadRequest)
}
