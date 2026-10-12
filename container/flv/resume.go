package flv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/nextpkg/streamd/internal/mediatime"
	"github.com/nextpkg/streamd/packet"
)

func (mixer *Mixer) Resume(reader io.ReaderAt, size int64) (uint32, error) {
	if size == 0 {
		mixer.timeline = mediatime.Tail{}
		return 0, mixer.SetFlvHeader()
	}
	offset, err := resumeTimestamp(reader, size)
	if err != nil {
		return 0, err
	}
	return mixer.ResumeAt(offset)
}

func (mixer *Mixer) ResumeAt(offset uint32) (uint32, error) {
	if offset == 0 {
		return 0, errors.New("cannot resume FLV at a zero timestamp")
	}
	mixer.timeline = mediatime.Tail{}
	for _, cached := range []*bytes.Buffer{mixer.cache.avcSeqHdr, mixer.cache.aacSeqHdr} {
		if cached.Len() == 0 {
			continue
		}
		header := bytes.Clone(cached.Bytes())
		binary.BigEndian.PutUint32(header[4:8], offset<<8|offset>>24)
		if _, err := mixer.flv.Write(header); err != nil {
			return 0, err
		}
	}
	return offset, nil
}

func resumeTimestamp(reader io.ReaderAt, size int64) (uint32, error) {
	var header [9]byte
	if _, err := reader.ReadAt(header[:], 0); err != nil {
		return 0, err
	}
	start := int64(binary.BigEndian.Uint32(header[5:]))
	if string(header[:3]) != "FLV" || header[3] != 1 || start < 9 || start+4 > size {
		return 0, errors.New("invalid existing FLV header")
	}
	var previousSize [4]byte
	if _, err := reader.ReadAt(previousSize[:], start); err != nil {
		return 0, err
	}
	if binary.BigEndian.Uint32(previousSize[:]) != 0 {
		return 0, errors.New("invalid initial FLV tag size")
	}
	var tail mediatime.Tail
	cursor := size
	for tags := 0; cursor > start+4 && tags < 256; tags++ {
		if _, err := reader.ReadAt(previousSize[:], cursor-4); err != nil {
			return 0, err
		}
		length := int64(binary.BigEndian.Uint32(previousSize[:]))
		position := cursor - 4 - length
		if length < tagHdrLen || position < start+4 {
			return 0, errors.New("invalid existing FLV tail length")
		}
		var tag [tagHdrLen]byte
		if _, err := reader.ReadAt(tag[:], position); err != nil {
			return 0, err
		}
		dataSize := int64(tag[1])<<16 | int64(tag[2])<<8 | int64(tag[3])
		if dataSize+tagHdrLen != length || tag[8] != 0 || tag[9] != 0 || tag[10] != 0 {
			return 0, errors.New("invalid existing FLV tag")
		}
		if tag[0] != packet.TagAudio && tag[0] != packet.TagVideo && tag[0] != packet.TagScriptDataAMF0 {
			return 0, errors.New("unsupported existing FLV tag")
		}
		if tag[0] == packet.TagVideo || tag[0] == packet.TagAudio {
			var prefix [5]byte
			prefixSize := int64(2)
			if tag[0] == packet.TagVideo {
				prefixSize = 5
			}
			if dataSize < prefixSize {
				return 0, errors.New("truncated FLV media header")
			}
			if _, err := reader.ReadAt(prefix[:prefixSize], position+tagHdrLen); err != nil {
				return 0, err
			}
			if (tag[0] == packet.TagVideo && prefix[0]&0x0f != 7) || (tag[0] == packet.TagAudio && prefix[0]>>4 != 10) {
				return 0, errors.New("unsupported existing FLV codec")
			}
			if prefix[1] == 1 {
				decoding := int64(tag[7])<<24 | int64(tag[4])<<16 | int64(tag[5])<<8 | int64(tag[6])
				presentation := decoding
				if tag[0] == packet.TagVideo {
					composition := int32(prefix[2])<<24 | int32(prefix[3])<<16 | int32(prefix[4])<<8
					presentation += int64(composition >> 8)
				}
				tail.ObserveReverse(int(tag[0]), decoding, presentation)
			}
		}
		cursor = position
	}
	return tail.Next()
}
