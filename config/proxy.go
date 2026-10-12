package config

import (
	"errors"
	"net/http"
	"strings"
)

// GetProxy HTTP反向代理
func GetProxy() *Proxy {
	return &Get().Proxy
}

// Proxy HTTP反向代理
type Proxy struct {
	Prefix       string `json:"prefix"`        // 路径前缀
	OnNotFound   string `json:"on_not_found"`  // 路径找不到时发起HTTP请求
	EnableSecure bool   `json:"enable_secure"` // 启动防盗链
	SecureKey    string `json:"secure_key"`    // 防盗链密钥
	SecureTTL    int64  `json:"secure_ttl"`    // 防盗链过期时间(单位: 秒)
	signatures   *SignatureSet
	prefixes     map[string]struct{}
}

func (proxy *Proxy) AcceptsPrefix(prefix string) bool {
	if prefix == "/"+strings.Trim(proxy.Prefix, "/") {
		return true
	}
	_, accepted := proxy.prefixes[prefix]
	return accepted
}

func (proxy *Proxy) preparePrefixes(previous *Proxy) {
	prefix := "/" + strings.Trim(proxy.Prefix, "/")
	if previous != nil {
		if _, exists := previous.prefixes[prefix]; exists {
			proxy.prefixes = previous.prefixes
			return
		}
	}
	proxy.prefixes = map[string]struct{}{prefix: {}}
	if previous != nil {
		for prefix := range previous.prefixes {
			proxy.prefixes[prefix] = struct{}{}
		}
	}
}

func (c *Proxy) validate() error {
	c.Prefix = strings.TrimSpace(c.Prefix)
	if c.Prefix == "" {
		return errors.New("empty proxy prefix")
	}
	for _, segment := range strings.Split(strings.Trim(c.Prefix, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("proxy prefix requires non-empty path segments without dot segments")
		}
	}
	if c.EnableSecure && (c.SecureKey == "" || c.SecureTTL <= 0 || c.SecureTTL > 7*24*60*60) {
		return errors.New("secure proxy requires a key and TTL between 1 second and 7 days")
	}

	return nil
}

// Check 检查防盗链
func (c *Proxy) Check(key, token string, e string) int {
	if !c.EnableSecure {
		return http.StatusOK
	}

	if c.signatures != nil {
		return c.signatures.Check(key, token, e)
	}
	return NewSignatureSet(c.SecureKey, c.SecureTTL).Check(key, token, e)
}
