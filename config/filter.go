package config

import (
	"net/url"

	"github.com/nextpkg/streamd/internal/logging"
)

// filter 字符串过滤器
type filter func(string) bool

// filterStringSlice 对字符串列表按filter规则去重
func filterStringSlice(tuple []string, filters ...filter) []string {
	seen := make(map[string]struct{})
	var result []string

Loop:
	// 去重
	for _, value := range tuple {
		for _, reject := range filters {
			if reject == nil {
				continue
			}

			if reject(value) {
				logging.Tracef("ignore string='%s'", value)
				continue Loop
			}
		}

		if _, exists := seen[value]; !exists {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	return result
}

// isEmpty 过滤空字符串
func isEmpty(s string) bool {
	return len(s) == 0
}

// isRTMP 判断是否为 RTMP 地址。
func isRTMP(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		logging.Debug(err)
		return false
	}

	if len(u.Host) == 0 {
		return false
	}

	return u.Scheme == "rtmp"
}

// isNotRTMP 过滤非RTMP字符串
func isNotRTMP(s string) bool {
	return !isRTMP(s)
}

// isHTTP 过滤HTTP字符串
func isHTTP(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		logging.Error(err)
		return false
	}

	if len(u.Host) == 0 {
		return false
	}

	return u.Scheme == "http" || u.Scheme == "https"
}
