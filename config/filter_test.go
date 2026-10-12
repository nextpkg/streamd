package config

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterIsHTTP(t *testing.T) {
	at := assert.New(t)

	at.False(isHTTP("http:127.0.0.1:1935/app/instance"))
	at.False(isHTTP("rtmp://127.0.0.1:1935/app/instance"))

	at.True(isHTTP("http://1270.0.1:1935/app/instance"))
	at.True(isHTTP("http://app/instance"))

	at.True(isHTTP("https://127.0.0.1:1935/app/instance"))
	at.True(isHTTP("http://127.0.0.1:1935/app/instance"))
}

func TestFilterIsRTMP(t *testing.T) {
	at := assert.New(t)

	at.False(isRTMP(""))
	at.False(isRTMP("/app/instance"))
	at.False(isRTMP("127.0.0.1/app/instance"))

	at.True(isRTMP("rtmp://127.0.0.1"))
	at.True(isRTMP("rtmp://127.0.0.1/app/instance"))
	at.True(isRTMP("rtmp://127.0.0.1/app/instance?to_url=url"))
	at.True(isRTMP("rtmp://127.0.0.1/path/to/another"))
}

func TestUniqueStringInTuple(t *testing.T) {
	at := assert.New(t)

	at.Equal([]string{"a"}, filterStringSlice([]string{"a", "a", "a"}))

	data := filterStringSlice([]string{"a", "b", "b", "", ""}, isEmpty)
	sort.Strings(data)

	at.Equal([]string{"a", "b"}, data)
	at.Equal([]string{"rtmp://127.0.0.1/app/instance"},
		filterStringSlice([]string{
			"rtmp://127.0.0.1/app/instance", "127.0.0.1:1234/app/instance", "/app/instance"}, isNotRTMP))

	at.Equal([]string{"rtmp://127.0.0.1/app/instance"},
		filterStringSlice([]string{"rtmp://127.0.0.1/app/instance", "", "/app/instance"}, isNotRTMP))
}
