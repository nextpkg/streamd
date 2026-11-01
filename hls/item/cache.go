package item

import (
	"bytes"
	"container/list"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/nextpkg/streamd/internal/logging"
)

// TsItem 可用TS列表中的项
type TsItem struct {
	name          string /* ts的名称, 一般是"url"中的"path"部分 */
	sequence      int64  /* ts对应的序列号 */
	duration      int64  /* ts播放时长, 单位: ms */
	data          []byte /* ts文件内容 */
	signed        bool
	discontinuity bool
}

// NewTsItem 复制 TS 数据并创建播放列表项
func NewTsItem(name string, duration, sequence int64, data []byte) *TsItem {
	parsed, _ := url.Parse(name)
	item := &TsItem{
		name:     name,
		sequence: sequence,
		duration: duration,
		data:     make([]byte, len(data)),
	}
	item.signed = parsed != nil && parsed.Query().Get("token") != ""

	copy(item.data, data)
	return item
}

func (t *TsItem) RequiresSignature() bool { return t.signed }

func (t *TsItem) Duration() time.Duration { return time.Duration(t.duration) * time.Millisecond }

// Response 响应TS数据包
func (t *TsItem) Response(w http.ResponseWriter) bool {
	// ===============================
	// 设置响应头
	// ===============================
	w.Header().Set("Content-Type", "video/mp2ts")
	w.Header().Set("Content-Length", strconv.Itoa(len(t.data)))
	// ===============================

	written, err := w.Write(t.data)
	if err != nil {
		logResponseError(err)
	}
	return err == nil && len(t.data) > 0 && written == len(t.data)
}

// TsItemCache 缓存可用TS列表
type TsItemCache struct {
	tsIndex           *list.List // 存放 <sequence>, 以保证能顺序读取到"tsStore"的内容
	tsStore           sync.Map   // 按 sequence 查找切片；遍历顺序由 tsIndex 维护
	playlistLength    int64      // M3U8 中含有ts的数量
	duration          atomic.Int64
	isEnd             atomic.Bool
	ready             atomic.Bool
	retired           map[string]retiredItem
	nextDiscontinuity bool
	sync.Mutex
}

type retiredItem struct {
	item    *TsItem
	expires time.Time
}

// NewTsItemCache 缓存可用TS列表
func NewTsItemCache(playlistLength int64) *TsItemCache {
	if playlistLength <= 0 {
		panic("playlistLength<=0")
	}

	return &TsItemCache{
		tsIndex:        list.New(),
		playlistLength: playlistLength,
		retired:        make(map[string]retiredItem),
	}
}

// GenerateM3u8 生成并返回M3U8播放列表
func (t *TsItemCache) GenerateM3u8() []byte {
	body, _ := t.generateM3u8()
	return body
}

func (t *TsItemCache) generateM3u8() ([]byte, time.Duration) {
	// 列表中最长的单片时长，单位：毫秒
	maxDuration := int64(0)

	// 第一个序列的序列号
	firstSlice := int64(0)

	// 遍历缓存链表, 保存持续时长最大的值以及第一片的序列号, 并且保存每一个ts文件的文件名和播放时长
	m3u8Body := bytes.NewBuffer(make([]byte, 0, 512))
	m3u8Data := bytes.NewBuffer(make([]byte, 0, 512))

	var data []*TsItem

	t.Lock()
	playlistLength := t.playlistLength
	for e := t.tsIndex.Front(); e != nil; e = e.Next() {
		if value, ok := t.tsStore.Load(e.Value); ok {
			data = append(data, value.(*TsItem))
		}
	}
	t.Unlock()

	if len(data) == 0 || (!t.ready.Load() && len(data)*2 <= int(playlistLength) && !t.isEnd.Load()) {
		return nil, 0
	}

	for index, item := range data {
		if item.discontinuity {
			m3u8Body.WriteString("#EXT-X-DISCONTINUITY\n")
		}

		// 找到最长播放时长
		if item.duration > maxDuration {
			maxDuration = item.duration
		}

		// 缓存第一个序列号
		if index == 0 {
			firstSlice = item.sequence
		}

		// 填充文件名以及时长
		_, err := fmt.Fprintf(m3u8Body, "#EXTINF:%.3f,\n%s\n", float64(item.duration)/1000, item.name)
		if err != nil {
			log.Error(err)
			continue
		}
	}

	if t.isEnd.Load() {
		m3u8Body.Write([]byte("#EXT-X-ENDLIST\n"))
	}

	targetDuration := (maxDuration + 999) / 1000

	// 填充M3U8的值, 并返回M3U8播放列表
	_, err := fmt.Fprintf(m3u8Data, `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:%d
#EXT-X-MEDIA-SEQUENCE:%d
%s`, targetDuration, firstSlice, m3u8Body.String())
	if err != nil {
		log.Error(err)
	}

	t.duration.Store(targetDuration * int64(len(data)))
	t.ready.Store(true)

	return m3u8Data.Bytes(), time.Duration(targetDuration*int64(len(data))) * time.Second
}

// Duration 返回最近生成列表的时长上界：目标片长乘以片数。
func (t *TsItemCache) Duration() time.Duration {
	return time.Duration(t.duration.Load()) * time.Second
}

func (t *TsItemCache) HasItems() bool { t.Lock(); defer t.Unlock(); return t.tsIndex.Len() != 0 }

func (t *TsItemCache) IsLive() bool { return !t.isEnd.Load() }

// Response 响应m3u8数据包
func (t *TsItemCache) Response(w http.ResponseWriter) time.Duration {
	m3u8, window := t.generateM3u8()
	if m3u8 == nil {
		http.Error(w, "playlist is preparing", http.StatusAccepted)
		return 0
	}

	// ===============================
	// 设置响应头
	// ===============================
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Type", "application/x-mpegURL")
	w.Header().Set("Content-Length", strconv.Itoa(len(m3u8)))
	// ===============================

	// 发送
	written, err := w.Write(m3u8)
	if err != nil {
		logResponseError(err)
	}
	if err != nil || written != len(m3u8) {
		return 0
	}
	return window
}

// SetItem 向缓存中插入一个ts数据
func (t *TsItemCache) SetItem(sequence string, item *TsItem) {
	t.Lock()
	defer t.Unlock()
	item.discontinuity = t.nextDiscontinuity
	t.nextDiscontinuity = false
	t.tsStore.Store(sequence, item)
	t.tsIndex.PushBack(sequence)
	t.trimLocked(time.Now(), t.playlistLength)
}

func (t *TsItemCache) trimLocked(now time.Time, previousLength int64) {
	for t.tsIndex.Len() > int(t.playlistLength) {
		front := t.tsIndex.Front()
		sequence := front.Value.(string)
		t.tsIndex.Remove(front)
		if value, ok := t.tsStore.LoadAndDelete(sequence); ok {
			item := value.(*TsItem)
			retention := max(t.Duration(), time.Duration(item.duration*previousLength)*time.Millisecond) + time.Duration(item.duration)*time.Millisecond
			t.retired[sequence] = retiredItem{item: item, expires: now.Add(retention)}
		}
	}
	for sequence, retired := range t.retired {
		if !now.Before(retired.expires) {
			delete(t.retired, sequence)
		}
	}
}

func (t *TsItemCache) Resize(length int64) {
	if length <= 0 {
		return
	}
	t.Lock()
	defer t.Unlock()
	previous := t.playlistLength
	t.playlistLength = length
	t.trimLocked(time.Now(), previous)
}

func (t *TsItemCache) MarkLive(discontinuity bool) {
	t.Lock()
	t.nextDiscontinuity = discontinuity
	t.isEnd.Store(false)
	t.Unlock()
}

func (t *TsItemCache) GetAvailableItem(sequence string) *TsItem {
	if item := t.GetItem(sequence); item != nil {
		return item
	}
	t.Lock()
	defer t.Unlock()
	retired, ok := t.retired[sequence]
	if !ok {
		return nil
	}
	if !time.Now().Before(retired.expires) {
		delete(t.retired, sequence)
		return nil
	}
	return retired.item
}

// GetItem 按key从缓存中取出对应的ts包
func (t *TsItemCache) GetItem(sequence string) *TsItem {
	item, ok := t.tsStore.Load(sequence)
	if !ok {
		return nil
	}

	return item.(*TsItem)
}

// MarkEnd 标记M3U8流已经结束
func (t *TsItemCache) MarkEnd() {
	t.isEnd.Store(true)
}

// Cleanup 清理缓存数据
func (t *TsItemCache) Cleanup() {
	t.Lock()
	defer t.Unlock()
	t.tsIndex.Init()
	clear(t.retired)

	// 清理数据
	t.tsStore.Range(func(key, value interface{}) bool {
		t.tsStore.Delete(key)
		return true
	})
}
