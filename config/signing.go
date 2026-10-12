package config

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type signingKey struct {
	secret string
	ttl    int64
	until  int64
}

type SignatureSet struct {
	mu       sync.RWMutex
	current  signingKey
	previous []signingKey
}

func NewSignatureSet(secret string, ttl int64) *SignatureSet {
	return &SignatureSet{current: signingKey{secret: secret, ttl: ttl}}
}

func (signatures *SignatureSet) next(secret string, ttl, now int64) *SignatureSet {
	signatures.mu.RLock()
	defer signatures.mu.RUnlock()
	result := NewSignatureSet(secret, ttl)
	for _, previous := range signatures.previous {
		if previous.until >= now {
			result.previous = append(result.previous, previous)
		}
	}
	if signatures.current.secret != "" && (secret != signatures.current.secret || ttl != signatures.current.ttl) {
		retired := signatures.current
		retired.until = now + retired.ttl
		result.previous = append(result.previous, retired)
	}
	return result
}

func (signatures *SignatureSet) Update(secret string, ttl int64) {
	next := signatures.next(secret, ttl, time.Now().Unix())
	signatures.mu.Lock()
	signatures.current, signatures.previous = next.current, next.previous
	signatures.mu.Unlock()
}

func (signatures *SignatureSet) Check(key, token, expiration string) int {
	return signatures.checkAt(key, token, expiration, time.Now().Unix())
}

func (signatures *SignatureSet) checkAt(key, token, expiration string, now int64) int {
	signatures.mu.RLock()
	defer signatures.mu.RUnlock()
	if signatures.current.secret == "" {
		return http.StatusOK
	}
	expires, err := strconv.ParseInt(expiration, 10, 64)
	if err != nil || expires <= 0 {
		return http.StatusForbidden
	}
	keys := append([]signingKey{signatures.current}, signatures.previous...)
	status := http.StatusForbidden
	for _, candidate := range keys {
		if subtle.ConstantTimeCompare([]byte(AntiLeechSign(key, candidate.secret, expires)), []byte(token)) != 1 {
			continue
		}
		if expires < now || (candidate.until != 0 && candidate.until < now) {
			status = http.StatusGone
			continue
		}
		limit := now + candidate.ttl
		if expires > limit {
			continue
		}
		return http.StatusOK
	}
	return status
}

func (proxy *Proxy) prepareSigning(previous *Proxy, now int64) {
	secret := proxy.SecureKey
	if !proxy.EnableSecure {
		secret = ""
	}
	if previous != nil && previous.signatures != nil {
		proxy.signatures = previous.signatures.next(secret, proxy.SecureTTL, now)
	} else {
		proxy.signatures = NewSignatureSet(secret, proxy.SecureTTL)
	}
}
