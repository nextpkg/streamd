package rtmp

import (
	"net/url"
	"testing"
)

func TestDiscoveredRTMPURLSelectsAuthorityWithoutChangingStreamIdentity(test *testing.T) {
	for advertisement, host := range map[string]string{
		"peer:31935":          "peer:31935",
		"2001:db8::1":         "[2001:db8::1]",
		"[fe80::1%eth0]:1935": "[fe80::1%eth0]:1935",
		`{"version":1,"rtmp":"rtmp.example:31935","http":"http.example:38080"}`: "rtmp.example:31935",
	} {
		address, err := discoveredRTMPURL("live", "nested/channel?not-a-query", advertisement)
		if err != nil {
			test.Fatal(err)
		}
		parsed, err := url.Parse(address)
		if err != nil || parsed.Scheme != "rtmp" || parsed.Host != host || parsed.Path != "/live/nested/channel?not-a-query" || parsed.RawQuery != "" {
			test.Fatalf("discovered RTMP URL changed identity: %q %v", address, err)
		}
	}
	for _, advertisement := range []string{"", "peer:0", `{"version":2,"rtmp":"peer","http":"peer"}`} {
		if _, err := discoveredRTMPURL("live", "sample", advertisement); err == nil {
			test.Fatalf("unusable discovery URL accepted: %q", advertisement)
		}
	}
}
