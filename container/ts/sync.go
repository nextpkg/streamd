package ts

// 音视频频率
const (
	// aacSL 每个 AAC 帧的采样数
	aacSL = 1024

	// avcHZ 每毫秒对应的 90 kHz 时钟刻度数
	avcHZ = 90
)

// sync 音视频同步
type sync struct {
	frameNum int64 // 累计帧数
	frameDts int64 // 基准时间
	syncMs   int64 // 允许的时间戳偏差，单位：90 kHz 时钟刻度
}

// newSync 音视频时间戳同步
func newSync(ms int64) *sync {
	if ms <= 0 {
		panic("ms<=0")
	}
	return &sync{
		syncMs: ms * avcHZ,
	}
}

// syncAudioTs 按 AAC 帧采样数平滑音频时间戳，超过阈值则重置基准。
// dts: 输入并原地更新音频时间戳，单位：90 kHz 时钟刻度
// sampleRate: 音频采样率, 单位: HZ
func (s *sync) syncAudioTs(dts *int64, sampleRate int) {
	// 根据采样率计算每个 AAC 帧的时长，转换为 90 kHz 时钟刻度
	tsIncrement := avcHZ * 1000 * aacSL / sampleRate
	pts := s.frameDts + s.frameNum*int64(tsIncrement)

	// 计算出pts和dts之间的差值
	var ptsDtsGap int64
	if pts >= *dts {
		ptsDtsGap = pts - *dts
	} else {
		ptsDtsGap = *dts - pts
	}

	// 差值在阈值内，dts=pts
	if ptsDtsGap <= s.syncMs {
		s.frameNum++
		*dts = pts
		return
	}

	// 差值在阈值外，dts=dts
	s.frameNum = 1
	s.frameDts = *dts
}
