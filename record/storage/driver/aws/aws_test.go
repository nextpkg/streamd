package aws

import (
	"bytes"
	"testing"

	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
)

func TestOriginalAWSExampleLocalContract(t *testing.T) {
	server := teststore.New(t, "test-bucket")
	client, err := objectstore.NewS3(&objectstore.S3Config{Enable: true, Endpoint: server.URL, Bucket: "test-bucket", SecretID: "test-id", SecretKey: "test-secret", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	key := "test/sample/2000000000.flv"
	driver := NewDriver(client, key, 900, "test-bucket", false)
	if err := driver.Open(); err != nil {
		t.Fatal(err)
	}
	var expected []byte
	for index, size := range []int{1024 * 1024, 1024*1024 - 2, 1024*1024 - 1} {
		data := bytes.Repeat([]byte{byte(index + 1)}, size)
		expected = append(expected, data...)
		if err := driver.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := driver.Close(true, true); err != nil {
		t.Fatal(err)
	}
	if actual := server.Download(key); !bytes.Equal(expected, actual) {
		t.Fatalf("downloaded %d bytes, expected exact %d-byte payload", len(actual), len(expected))
	}
}
