package cos

import (
	"bytes"
	"testing"

	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/internal/teststore"
)

func TestOriginalCOSExampleLocalContract(t *testing.T) {
	server := teststore.New(t, "")
	client, err := objectstore.NewCOS(&objectstore.COSConfig{Enable: true, Endpoint: server.URL, SecretID: "test-id", SecretKey: "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	key := "test/sample/2000000000.flv"
	driver := NewDriver(client, key, 900, false)
	if err := driver.Open(); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0x35}, 1024*1024)
	if err := driver.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(true, true); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(server.Download(key), data) {
		t.Fatal("COS recorded payload differs from input")
	}
}
