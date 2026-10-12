package cos

import (
	"github.com/nextpkg/streamd/internal/objectstore"
	"github.com/nextpkg/streamd/record/storage/driver"
)

func NewDriver(client *objectstore.Client, key string, segment int64, resume bool) driver.Driver {
	return driver.NewObject(client, key, segment, resume)
}
