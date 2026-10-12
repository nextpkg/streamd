package aws

import (
	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/record/storage/driver"
)

func NewDriver(client *objectstore.Client, key string, segment int64, bucket string, resume bool) driver.Driver {
	return driver.NewObject(client, key, segment, resume)
}
