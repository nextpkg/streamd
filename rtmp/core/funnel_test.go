package core

import (
	"bytes"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/assert"
)

type testClient struct {
	*Universal
	buf     bytes.Buffer
	before  bool
	after   bool
	written chan struct{}
}

func newTestClient() *testClient {
	info := base.NewInfo("t_app", "t_ins", false)

	return &testClient{
		Universal: NewUniversal(info),
		written:   make(chan struct{}),
	}
}

func (c *testClient) Write(p *packet.Packet) error {
	_, err := c.buf.Write(p.Data)
	close(c.written)
	return err
}

func (client *testClient) Name() string {
	return "test"
}

func (c *testClient) Before() {
	c.before = true
}

func (c *testClient) After() {
	c.after = true
}

func TestNewStream(t *testing.T) {
	at := assert.New(t)

	c := newTestClient()
	s := NewFunnel(c)

	err := s.Write(&packet.Packet{
		Data: make([]byte, 1024),
	})
	at.Nil(err)

	<-c.written

	at.True(c.before)
	at.False(c.after)

	s.Close()
	s.Wait()

	at.Equal(1024, c.buf.Len())

	at.True(c.before)
	at.True(c.after)

	s.Wait()
}
