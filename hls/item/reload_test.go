package item

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPlaylistResizeRetainsIssuedSegments(t *testing.T) {
	cache := NewTsItemCache(2)
	first := NewTsItem("sample/1.ts?token=old&e=1000", 2000, 1, []byte{1})
	cache.SetItem("1", first)
	cache.SetItem("2", NewTsItem("sample/2.ts", 2000, 2, []byte{2}))
	require.NotEmpty(t, cache.GenerateM3u8())
	cache.Resize(8)
	require.NotEmpty(t, cache.GenerateM3u8(), "window growth must not revert a live playlist to preparing")
	cache.Resize(1)
	require.Nil(t, cache.GetItem("1"))
	require.Same(t, first, cache.GetAvailableItem("1"))
	require.True(t, first.RequiresSignature())
	require.False(t, cache.GetItem("2").RequiresSignature())
	cache.Lock()
	retired := cache.retired["1"]
	retired.expires = time.Now().Add(-time.Second)
	cache.retired["1"] = retired
	cache.Unlock()
	require.Nil(t, cache.GetAvailableItem("1"))
	cache.MarkEnd()
	cache.MarkLive(true)
	cache.SetItem("3", NewTsItem("sample/3.ts", 2000, 3, []byte{3}))
	playlist := string(cache.GenerateM3u8())
	require.Contains(t, playlist, "#EXT-X-MEDIA-SEQUENCE:3\n")
	require.Contains(t, playlist, "#EXT-X-DISCONTINUITY\n")
	require.NotContains(t, playlist, "#EXT-X-ENDLIST")
	cache.Cleanup()
	require.False(t, cache.HasItems())
	require.Nil(t, cache.GetAvailableItem("3"))
}

func TestPlaylistConcurrentResizeAndRead(t *testing.T) {
	cache := NewTsItemCache(2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for index := int64(1); index <= 200; index++ {
			sequence := fmt.Sprint(index)
			cache.SetItem(sequence, NewTsItem(sequence+".ts", 1000, index, []byte{1}))
			cache.Resize(index%8 + 1)
		}
	}()
	go func() {
		defer workers.Done()
		for index := 1; index <= 200; index++ {
			cache.GenerateM3u8()
			cache.GetAvailableItem(fmt.Sprint(index))
		}
	}()
	workers.Wait()
	require.NotEmpty(t, cache.GenerateM3u8())
}
