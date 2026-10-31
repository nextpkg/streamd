package item

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTsItemCache(t *testing.T) {
	at := assert.New(t)

	ts := NewTsItemCache(2)
	t.Cleanup(ts.Cleanup)
	at.Nil(ts.GenerateM3u8())

	item1 := NewTsItem("t_app/t_ins/1.ts?test=123", 4000, 1, []byte{1, 2, 3})
	ts.SetItem("1", item1)
	at.Equal(item1, ts.GetItem("1"))
	at.Nil(ts.GetItem("2"))
	at.Nil(ts.GenerateM3u8(), "a half-full initial window is not ready")

	item2 := NewTsItem("t_app/t_ins/2.ts?test=123", 4000, 2, []byte{1, 2, 3})
	ts.SetItem("2", item2)
	at.Equal([]byte(`#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:1
#EXTINF:4.000,
t_app/t_ins/1.ts?test=123
#EXTINF:4.000,
t_app/t_ins/2.ts?test=123
`), ts.GenerateM3u8())

	item3 := NewTsItem("t_app/t_ins/3.ts?test=123", 4000, 3, []byte{1, 2, 3})
	ts.SetItem("3", item3)
	at.Equal([]byte(`#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:2
#EXTINF:4.000,
t_app/t_ins/2.ts?test=123
#EXTINF:4.000,
t_app/t_ins/3.ts?test=123
`), ts.GenerateM3u8())

	at.Nil(ts.GetItem("1"), "retired segment must leave the live window")
	playlist := string(ts.GenerateM3u8())
	ts.MarkEnd()
	at.Equal(playlist+"#EXT-X-ENDLIST\n", string(ts.GenerateM3u8()))
	ts.Cleanup()
	at.Nil(ts.GenerateM3u8())
}

func TestPlaylistTargetDurationRoundsUp(t *testing.T) {
	cache := NewTsItemCache(1)
	t.Cleanup(cache.Cleanup)
	cache.SetItem("7", NewTsItem("live/7.ts", 5001, 7, []byte{1}))
	assert.Equal(t, `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:7
#EXTINF:5.001,
live/7.ts
`, string(cache.GenerateM3u8()))
}
