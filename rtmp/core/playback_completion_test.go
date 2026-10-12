package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nextpkg/goav/amf"
	"github.com/nextpkg/streamd/packet"
)

func TestPlaybackCompletionFollowsAllQueuedMedia(test *testing.T) {
	for _, method := range []string{"play", "publish"} {
		test.Run(method, func(test *testing.T) {
			server, peer := net.Pipe()
			connection := NewConn(server, 4096)
			writer := NewWriter(&ConnServer{conn: connection, method: method, streamID: 7})
			test.Cleanup(func() { _ = peer.Close(); writer.Abort(); writer.Wait() })
			if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				test.Fatal(err)
			}
			var chunks []*ChunkStream
			var readError error
			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				reader := NewConn(peer, 4096)
				for {
					chunk, err := reader.getIntactChunk()
					if err != nil {
						readError = err
						return
					}
					copy := *chunk
					copy.data = append([]byte{}, chunk.data...)
					chunks = append(chunks, &copy)
				}
			}()
			for index := range 3 {
				if err := writer.Write(&packet.Packet{Type: packet.PktAudio, StreamID: 7, Data: []byte{0xaf, 1, byte(index)}, TimeStamp: uint32(index * 23)}); err != nil {
					test.Fatal(err)
				}
			}
			writer.Close()
			writer.Wait()
			<-readDone
			if !errors.Is(readError, io.EOF) {
				test.Fatalf("playback did not close cleanly: %v", readError)
			}
			expected := 3
			if method == "play" {
				expected = 5
			}
			if len(chunks) != expected {
				test.Fatalf("completion after media: got %d chunks, want %d", len(chunks), expected)
			}
			for index := range 3 {
				if chunks[index].typeID != packet.TagAudio || !bytes.Equal(chunks[index].data, []byte{0xaf, 1, byte(index)}) || chunks[index].streamID != 7 {
					test.Fatal("queued media changed before completion")
				}
			}
			if method != "play" {
				return
			}
			end := chunks[3]
			if end.typeID != 4 || len(end.data) != 6 || binary.BigEndian.Uint16(end.data) != streamEOF || binary.BigEndian.Uint32(end.data[2:]) != 7 {
				test.Fatal("missing stream-specific EOF control event")
			}
			status := chunks[4]
			values, err := amf.NewEnDecAMF0().DecodeBatch(bytes.NewReader(status.data))
			if err != nil || status.typeID != 20 || status.streamID != 7 || len(values) != 4 || values[0] != "onStatus" {
				test.Fatalf("invalid completion command: %v %v", values, err)
			}
			event, ok := values[3].(amf.Object)
			if !ok || event["level"] != "status" || event["code"] != "NetStream.Play.Stop" {
				test.Fatalf("invalid completion event: %v", values[3])
			}
		})
	}
}

func TestPlaybackCompletionCannotBlockRetirement(test *testing.T) {
	for _, abort := range []bool{false, true} {
		server, peer := net.Pipe()
		writer := NewWriter(&ConnServer{conn: NewConn(server, 128), method: "play", streamID: 1})
		writer.drainTimeout = 50 * time.Millisecond
		if abort {
			writer.Abort()
		} else {
			writer.Close()
		}
		done := make(chan struct{})
		go func() { writer.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			_ = peer.Close()
			<-done
			test.Fatal("unread completion blocked retirement")
		}
		if abort {
			var body [1]byte
			if count, err := peer.Read(body[:]); count != 0 || !errors.Is(err, io.EOF) {
				test.Fatalf("abort sent a successful completion: count=%d err=%v", count, err)
			}
		}
		_ = peer.Close()
	}
}
