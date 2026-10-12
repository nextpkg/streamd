package ts

import (
	"errors"
	"io"

	"github.com/nextpkg/streamd/internal/mediatime"
)

func (mixer *Mixer) Resume(reader io.ReaderAt, size int64) (uint32, error) {
	if size == 0 {
		mixer.timeline = mediatime.Tail{}
		return 0, mixer.SetTsHeader()
	}
	offset, err := resumeTimestamp(reader, size)
	if err != nil {
		return 0, err
	}
	return mixer.ResumeAt(offset)
}

func (mixer *Mixer) ResumeAt(offset uint32) (uint32, error) {
	if offset == 0 {
		return 0, errors.New("cannot resume TS at a zero timestamp")
	}
	mixer.timeline = mediatime.Tail{}
	mixer.muxer, mixer.sync = NewMuxer(), newSync(10)
	for _, pid := range []uint16{0x11, 0, 0x1001, videoPID, audioPID} {
		var discontinuity [tsPacketLen]byte
		for index := range discontinuity {
			discontinuity[index] = 0xff
		}
		discontinuity[0], discontinuity[1], discontinuity[2] = 0x47, byte(pid>>8), byte(pid)
		discontinuity[3], discontinuity[4], discontinuity[5] = 0x2f, 183, 0x80
		if pid == videoPID || pid == audioPID {
			discontinuity[3] = 0x20
		}
		if _, err := mixer.ts.Write(discontinuity[:]); err != nil {
			return 0, err
		}
	}
	return offset, mixer.SetTsHeader()
}

func resumeTimestamp(reader io.ReaderAt, size int64) (uint32, error) {
	if size%tsPacketLen != 0 {
		return 0, errors.New("existing TS file ends with a partial packet")
	}
	var tail mediatime.Tail
	counts := make(map[int]int)
	payloadBytes := make(map[int]int)
	buffer := make([]byte, tsPacketLen*1024)
	remaining := size
	for scanned := int64(0); remaining > 0 && scanned < 32<<20; {
		length := min(int64(len(buffer)), remaining)
		remaining -= length
		if _, err := reader.ReadAt(buffer[:length], remaining); err != nil {
			return 0, err
		}
		for index := int(length) - tsPacketLen; index >= 0; index -= tsPacketLen {
			packet := buffer[index : index+tsPacketLen]
			if packet[0] != 0x47 || packet[1]&0x80 != 0 || packet[3]&0xc0 != 0 || packet[3]&0x30 == 0 {
				return 0, errors.New("invalid existing TS packet")
			}
			pid := int(packet[1]&0x1f)<<8 | int(packet[2])
			if pid != videoPID && pid != audioPID {
				continue
			}
			start := 4
			if packet[3]&0x20 != 0 {
				start += 1 + int(packet[4])
			}
			if start > tsPacketLen || (packet[3]&0x10 != 0 && start == tsPacketLen) {
				return 0, errors.New("invalid existing TS adaptation length")
			}
			if packet[3]&0x10 == 0 {
				continue
			}
			payloadBytes[pid] += tsPacketLen - start
			if packet[1]&0x40 == 0 {
				continue
			}
			if start+14 > tsPacketLen {
				return 0, errors.New("truncated TS PES timestamp")
			}
			payload := packet[start:]
			if payload[0] != 0 || payload[1] != 0 || payload[2] != 1 || payload[7]&0x80 == 0 || payload[8] < 5 {
				return 0, errors.New("invalid TS PES header")
			}
			declared := int(payload[4])<<8 | int(payload[5])
			if declared != 0 && payloadBytes[pid] != declared+6 {
				return 0, errors.New("existing TS PES length does not match recorded payload")
			}
			payloadBytes[pid] = 0
			presentation, err := decodeTimestamp(payload[9:14])
			if err != nil {
				return 0, err
			}
			decoding := presentation
			if payload[7]&0x40 != 0 {
				if len(payload) < 19 || payload[8] < 10 {
					return 0, errors.New("truncated TS decoding timestamp")
				}
				decoding, err = decodeTimestamp(payload[14:19])
				if err != nil {
					return 0, err
				}
			}
			tail.ObserveReverse(pid, decoding/avcHZ, presentation/avcHZ)
			counts[pid]++
		}
		scanned += length
		if counts[videoPID] >= 32 && counts[audioPID] >= 32 {
			break
		}
	}
	for pid, pending := range payloadBytes {
		if pending != 0 && counts[pid] == 0 {
			return 0, errors.New("existing TS tail has no recoverable PES start")
		}
	}
	return tail.Next()
}

func decodeTimestamp(encoded []byte) (int64, error) {
	if encoded[0]&1 == 0 || encoded[2]&1 == 0 || encoded[4]&1 == 0 {
		return 0, errors.New("invalid TS timestamp markers")
	}
	return int64(encoded[0]&0x0e)<<29 | int64(encoded[1])<<22 | int64(encoded[2]&0xfe)<<14 | int64(encoded[3])<<7 | int64(encoded[4])>>1, nil
}
