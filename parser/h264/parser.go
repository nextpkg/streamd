package h264

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// Parser H264解析器
type Parser struct {
	specificInfo []byte        /* {0: sps, 1: pps}, 均包含start code */
	spsPps       *bytes.Buffer /* sps和pps共用, 均包含start code */
}

// NewParser 初始化h264解析器(pps/sps)
func NewParser() *Parser {
	return &Parser{
		spsPps: bytes.NewBuffer(make([]byte, maxSpsPpsLen)),
	}
}

// Parse 将H264打包格式转换为 Annex-b 的网络流格式, 写入w中
func (p *Parser) Parse(b []byte, isSeqHdr bool, w io.Writer) error {
	if len(b) == 0 || w == nil {
		return errors.New("no data to parse or nil writer")
	}

	// [AVCC格式]如果是序列头, 则解析出SPS和PPS
	if isSeqHdr {
		return p.parseSpecificInfo(b)
	}

	// [Annex-b格式]直接写入以Nalu开头的数据
	if p.isStartAtNaluHeader(b) {
		_, err := w.Write(b)
		if err != nil {
			return err
		}

		return nil
	}

	// [AVCC格式]转换为Annex-b格式并写入数据
	return p.getAnnexbH264(b, w)
}

// [AVCC格式]向specificInfo填充SPS和PPS, specificInfo的值: {0: sps数据, 1: pps数据}
func (p *Parser) parseSpecificInfo(src []byte) error {
	if len(src) < 9 {
		return errors.New("incomplete data, len(src)<9")
	}

	var sps []byte
	var pps []byte

	var seq sequenceHeader

	// 填充 AVCDecoderConfigurationRecord
	seq.configurationVersion = src[0] /* NALU头, 版本号: 1 */
	seq.avcProfileIndication = src[1] /* H264的profile, Baseline: 视频会议, Main: 标清电视, High: 高清电视 */
	seq.profileCompatility = src[2]
	seq.avcLevelIndication = src[3] /* h264的Level, 可以指定最大分辨率, 帧率等 */
	seq.reserved1 = src[4] >> 2
	seq.naluLen = src[4]&0x3 + 1
	seq.reserved2 = src[5] >> 5

	// 提取SPS
	seq.spsNum = src[5] & 0x1f                /* [3:7]SPS数量, 一般为1 */
	seq.spsLen = int(src[6])<<8 | int(src[7]) /* [0:15]SPS长度 */
	if len(src[8:]) < seq.spsLen || seq.spsLen <= 0 {
		return errors.New("incomplete sps data")
	}
	sps = append(sps, startCode...)
	sps = append(sps, src[8:(8+seq.spsLen)]...)

	// 提取PPS
	tmpBuf := src[(8 + seq.spsLen):]
	if len(tmpBuf) < 4 {
		return errors.New("incomplete pps header")
	}
	seq.ppsNum = tmpBuf[0]                          /* PPS数量 */
	seq.ppsLen = int(tmpBuf[1])<<8 | int(tmpBuf[2]) /* [0:15]PPS长度 */
	if len(tmpBuf[3:]) < seq.ppsLen || seq.ppsLen <= 0 {
		return errors.New("incomplete pps data")
	}
	pps = append(pps, startCode...)
	pps = append(pps, tmpBuf[3:]...)

	// 向specificInfo填充SPS和PPS
	p.specificInfo = append(p.specificInfo, sps...)
	p.specificInfo = append(p.specificInfo, pps...)

	return nil
}

// 判断数据是否是以NALU头开始, Annex-b格式以NALU头开始
func (p *Parser) isStartAtNaluHeader(src []byte) bool {
	if len(src) < naluBytesLen {
		return false
	}

	return src[0] == 0x00 && src[1] == 0x00 && src[2] == 0x00 && src[3] == 0x01
}

// [AVCC格式] 提取NALU的长度
func (p *Parser) naluSize(src []byte) (int, error) {
	if len(src) < naluBytesLen {
		return 0, errors.New("[hvcc]incomplete nalu data")
	}

	// [0:31] nalu单元的长度，不包括长度字段
	buf := src[:naluBytesLen]
	l := len(buf)

	var size = 0
	for i := 0; i < l; i++ {
		size = size<<8 + int(buf[i])
	}

	return size, nil
}

func (p *Parser) writeIDR(w io.Writer, hasSpsPps bool) error {

	if hasSpsPps {

		// 根据pps, 写入 SPS/PPS
		if _, err := w.Write(p.spsPps.Bytes()); err != nil {
			return err
		}
	} else {

		// 根据specificInfo, 写入 SPS和PPS
		if _, err := w.Write(p.specificInfo); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) writeSEI(w io.Writer, data []byte) error {

	// 写入 start code
	if _, err := w.Write(startCode); err != nil {
		return err
	}

	// 写入 sei 数据
	if _, err := w.Write(data); err != nil {
		return err
	}

	return nil
}

func (p *Parser) writePPS(data []byte) error {

	// 写入 start code
	if _, err := p.spsPps.Write(startCode); err != nil {
		return err
	}

	// 写入 SPS 或 PPS 的数据
	if _, err := p.spsPps.Write(data); err != nil {
		return err
	}

	return nil
}

// [AVCC->Annex-b]将以 AVCC 作为打包格式转换为以 Annex-b 作为打包格式的H264数据写入w中
func (p *Parser) getAnnexbH264(src []byte, w io.Writer) error {
	if len(src) < naluBytesLen {
		return errors.New("incomplete h264 header")
	}
	p.spsPps.Reset()
	var units [][]byte
	hasIDR := false
	for offset := 0; offset < len(src); {
		length, err := p.naluSize(src[offset:])
		if err != nil {
			return err
		}
		offset += naluBytesLen
		if length <= 0 || length > len(src)-offset {
			return errors.New("invalid nalu body")
		}
		unit := src[offset : offset+length]
		switch kind := unit[0] & 0x1f; kind {
		case naluTypeAud:
		case naluTypeIdr:
			hasIDR = true
			fallthrough
		case naluTypeSlice, naluTypeSei, naluTypeFiller:
			units = append(units, unit)
		case naluTypeSps, naluTypePps:
			if err := p.writePPS(unit); err != nil {
				return err
			}
		default:
			return fmt.Errorf("incompatible nalu type number=%d", kind)
		}
		offset += length
	}
	if _, err := w.Write(naluAud); err != nil {
		return err
	}
	if hasIDR {
		if err := p.writeIDR(w, p.spsPps.Len() != 0); err != nil {
			return err
		}
	}
	for _, unit := range units {
		if err := p.writeSEI(w, unit); err != nil {
			return err
		}
	}
	return nil
}
