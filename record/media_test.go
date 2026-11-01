package record

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	containerflv "github.com/nextpkg/streamd/container/flv"
	"github.com/nextpkg/streamd/packet"
	recordflv "github.com/nextpkg/streamd/record/flv"
	"github.com/nextpkg/streamd/record/storage"
	recordts "github.com/nextpkg/streamd/record/ts"
	"github.com/nextpkg/streamd/rtmp/core"
	"github.com/stretchr/testify/require"
)

func TestRecordingFinalizesMedia(t *testing.T) {
	previous := config.Get().Metadata
	config.Get().Metadata = config.Metadata{Provider: "test-provider1", Service: "Streaming media process service"}
	t.Cleanup(func() { config.Get().Metadata = previous })
	for _, format := range []struct {
		name      string
		newStream func(*base.Info, *storage.Storage) *core.Funnel
		verify    func(*testing.T, []byte, []byte, []byte)
	}{{"flv", recordflv.NewStream, verifyRecordedFLV}, {"ts", recordts.NewStream, verifyRecordedTS}} {
		for _, keyframe := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/keyframe=%v", format.name, keyframe), func(t *testing.T) {
				directory := t.TempDir()
				info := base.NewInfo("media", "sample", false)
				store := storage.NewStorage(info, &config.AppRecorder{Enable: true, SaveTo: directory, SaveAs: format.name, SaveMode: "round", Driver: "fs", Segment: 900}, storage.NewEvent())
				stream := format.newStream(info, store)
				t.Cleanup(func() { stream.Close(); stream.Wait() })
				video := []byte{0x17, 1, 0, 0, 0, 0, 0, 0, 1, 0x65}
				audio := []byte{0xaf, 1, 0x21, 0, 0x49, 0x90, 2, 0x19, 0, 0x23, 0x80}
				frames := []*packet.Packet{{Type: packet.PktVideo, Data: []byte{0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64, 0x00,
					0x1f, 0xff, 0xe1, 0x00, 0x19, 0x67, 0x64, 0x00,
					0x1f, 0xac, 0x24, 0x84, 0x01, 0x40, 0x16, 0xec,
					0x04, 0x40, 0x00, 0x00, 0x03, 0x00, 0x40, 0x00,
					0x00, 0x0c, 0x23, 0xc6, 0x0c, 0x92, 0x01, 0x00,
					0x05, 0x68, 0xee, 0x32, 0xc8, 0xb0}}}
				if keyframe {
					frames = append(frames, &packet.Packet{Type: packet.PktVideo, Data: video})
				}
				frames = append(frames, &packet.Packet{Type: packet.PktAudio, Data: []byte{0xaf, 0, 0x12, 0x10}}, &packet.Packet{Type: packet.PktAudio, Data: audio})
				for _, p := range frames {
					require.NoError(t, containerflv.NewDemuxer().Demux(p))
					require.NoError(t, stream.Write(p))
				}
				stream.Close()
				stream.Wait()
				files, err := filepath.Glob(filepath.Join(directory, "media", "sample", "*."+format.name))
				require.NoError(t, err)
				if !keyframe {
					require.Empty(t, files, "headers and preroll must not create an unplayable recording")
					return
				}
				require.Len(t, files, 1)
				data, err := os.ReadFile(files[0])
				require.NoError(t, err)
				last, err := store.ReadLastFile()
				require.NoError(t, err)
				require.Equal(t, data, last)
				format.verify(t, data, video, audio)
			})
		}
	}
}

// Inspect persisted tags independently of the service's FLV muxer.
func verifyRecordedFLV(t *testing.T, data, video, audio []byte) {
	t.Helper()
	require.Len(t, data, 245)
	require.Equal(t, []byte{'F', 'L', 'V', 1}, data[:4])
	offset := int(binary.BigEndian.Uint32(data[5:9]))
	require.Zero(t, binary.BigEndian.Uint32(data[offset:offset+4]))
	offset += 4
	var videos, audios [][]byte
	for offset < len(data) {
		require.GreaterOrEqual(t, len(data)-offset, 15)
		size := int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		end := offset + 11 + size
		require.LessOrEqual(t, end+4, len(data))
		body := data[offset+11 : end]
		if data[offset] == 9 {
			videos = append(videos, body)
		}
		if data[offset] == 8 {
			audios = append(audios, body)
		}
		require.Equal(t, uint32(11+size), binary.BigEndian.Uint32(data[end:end+4]), "previous tag size")
		offset = end + 4
	}
	require.Len(t, videos, 2)
	require.Equal(t, byte(0), videos[0][1], "AVC configuration precedes video")
	require.Equal(t, video, videos[1])
	require.Len(t, audios, 1)
	require.Equal(t, audio, audios[0])
}

func verifyRecordedTS(t *testing.T, data, video, audio []byte) {
	t.Helper()
	require.Len(t, data, 940)
	payloads := map[uint16][]byte{}
	for offset := 0; offset < len(data); offset += 188 {
		require.Equal(t, byte(0x47), data[offset], "TS sync at %d", offset)
		require.Zero(t, data[offset+1]&0x80, "transport error indicator")
		pid := binary.BigEndian.Uint16(data[offset+1:offset+3]) & 0x1fff
		start := offset + 4
		if data[offset+3]&0x20 != 0 {
			start += 1 + int(data[offset+4])
		}
		require.LessOrEqual(t, start, offset+188)
		if data[offset+3]&0x10 != 0 {
			payloads[pid] = append(payloads[pid], data[start:offset+188]...)
		}
	}
	require.NotEmpty(t, payloads[0], "PAT missing")
	require.NotEmpty(t, payloads[0x100], "video missing")
	require.NotEmpty(t, payloads[0x101], "audio missing")
	require.True(t, bytes.Contains(payloads[0x100], append([]byte{0, 0, 0, 1}, video[9:]...)), "IDR payload missing")
	require.True(t, bytes.Contains(payloads[0x101], audio[2:]), "AAC payload missing")
}
