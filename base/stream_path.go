package base

import (
	"strings"
	"unicode/utf8"
)

func ValidAppName(name string) bool {
	return !strings.Contains(name, "/") && ValidStreamName(name)
}

func ValidStreamName(name string) bool {
	if !utf8.ValidString(name) {
		return false
	}
	for component := range strings.SplitSeq(name, "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
		for _, character := range component {
			if character < 0x20 || character == 0x7f || character == '\\' {
				return false
			}
		}
	}
	return true
}
