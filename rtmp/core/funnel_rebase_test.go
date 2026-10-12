package core

import (
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/packet"
	"github.com/stretchr/testify/require"
)

type rebaseTerminal struct {
	*Universal
	entered    chan struct{}
	release    chan struct{}
	timestamps []uint32
}

func (terminal *rebaseTerminal) Name() string { return "rebase-test" }

func (terminal *rebaseTerminal) Write(frame *packet.Packet) error {
	if len(terminal.timestamps) == 0 {
		close(terminal.entered)
		<-terminal.release
	}
	terminal.timestamps = append(terminal.timestamps, frame.Baseline)
	return nil
}

func TestFunnelRebaseIsOrderedAfterQueuedMedia(t *testing.T) {
	terminal := &rebaseTerminal{Universal: NewUniversal(base.NewInfo("live", t.Name(), false)),
		entered: make(chan struct{}), release: make(chan struct{})}
	funnel := NewFunnel(terminal)
	require.NoError(t, funnel.Write(&packet.Packet{Type: packet.PktVideo, TimeStamp: 1000}))
	<-terminal.entered
	require.NoError(t, funnel.Write(&packet.Packet{Type: packet.PktVideo, TimeStamp: 1040}))
	funnel.RebaseTime()
	require.NoError(t, funnel.Write(&packet.Packet{Type: packet.PktVideo, TimeStamp: 0}))
	close(terminal.release)
	funnel.Close()
	funnel.Wait()
	require.Len(t, terminal.timestamps, 3)
	require.Equal(t, []uint32{1000, 1040}, terminal.timestamps[:2])
	require.Greater(t, terminal.timestamps[2], terminal.timestamps[1])
}
