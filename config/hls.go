package config

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

// AntiLeechSign 基于Key的防盗链的签名
func AntiLeechSign(key, secret string, expire int64) string {
	if expire == 0 {
		expire = time.Now().Unix() + 60
	}

	md5Sum := md5.Sum([]byte(fmt.Sprintf("%s %s %d", key, secret, expire)))

	sign := base64.StdEncoding.EncodeToString(md5Sum[:])
	sign = strings.Replace(sign, "+", "-", -1)
	sign = strings.Replace(sign, "/", "_", -1)
	sign = strings.Replace(sign, "=", "", -1)

	return sign
}

// AppHLS 控制HLS功能
type AppHLS struct {
	Enable         bool   `json:"enable"`
	Fragment       int64  `json:"fragment"`        // M3U8每个播放项的时间长度(单位: 秒)
	PlaylistLength int64  `json:"playlist_length"` // M3U8播放列表项目条数
	SecureKey      string `json:"secure_key"`      // 防盗链密钥
	SecureTTL      int64  `json:"secure_ttl"`      // 防盗链过期时间(单位: 秒)
}

// AntiLeechQuery 防盗链
func (a *AppHLS) AntiLeechQuery(key string) string {
	if a.SecureKey == "" {
		return ""
	}
	expire := time.Now().Unix() + a.SecureTTL

	sign := AntiLeechSign(key, a.SecureKey, expire)
	return fmt.Sprintf("token=%s&e=%d", sign, expire)
}

func (a *AppHLS) validate() error {
	if !a.Enable {
		return nil
	}

	if a.PlaylistLength <= 0 {
		return fmt.Errorf("`playlist_length`<=0")
	}

	if a.Fragment <= 0 {
		return fmt.Errorf("`fragment`<=0")
	}
	if a.SecureKey != "" && (a.SecureTTL <= 0 || a.SecureTTL > 7*24*60*60) {
		return fmt.Errorf("HLS signature TTL must be between 1 second and 7 days")
	}

	return nil
}
