package file

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileDriver(t *testing.T) {
	at := assert.New(t)

	f, err := os.CreateTemp(t.TempDir(), "recording")
	at.Nil(err)
	at.Nil(f.Close())

	data := []byte("test file")

	fd := NewDriver(f.Name())
	at.Nil(fd.Open())
	at.Nil(fd.Write(data))
	at.Nil(fd.Close(true, true))

	buf, err1 := os.ReadFile(f.Name())
	at.Nil(err1)
	at.Equal(data, buf)
}
