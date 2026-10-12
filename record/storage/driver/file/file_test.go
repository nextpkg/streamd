package file

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileDriver(t *testing.T) {
	at := assert.New(t)

	f, err := ioutil.TempFile("", "")
	at.Nil(err)
	at.Nil(f.Close())

	data := []byte("test file")

	fd := NewDriver(f.Name())
	at.Nil(fd.Open())
	at.Nil(fd.Write(data))
	at.Nil(fd.Close(true, true))

	buf, err1 := ioutil.ReadFile(f.Name())
	at.Nil(err1)
	at.Equal(data, buf)
}
