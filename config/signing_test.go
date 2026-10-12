package config

import (
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSignatureRotationBounds(t *testing.T) {
	const now int64 = 1000
	original := NewSignatureSet("old", 120)
	rotated := original.next("new", 30, now)
	for _, test := range []struct {
		name      string
		secret    string
		expires   int64
		checkTime int64
		want      int
	}{
		{"old link", "old", 1100, 1005, http.StatusOK},
		{"new link", "new", 1030, 1005, http.StatusOK},
		{"forged link", "wrong", 1030, 1005, http.StatusForbidden},
		{"expired link", "old", 1001, 1005, http.StatusGone},
		{"extended old key", "old", 1200, 1005, http.StatusForbidden},
		{"extended new key", "new", 1100, 1005, http.StatusForbidden},
		{"retired key", "old", 1100, 1121, http.StatusGone},
	} {
		t.Run(test.name, func(t *testing.T) {
			token := AntiLeechSign("live/sample", test.secret, test.expires)
			require.Equal(t, test.want, rotated.checkAt("live/sample", token, strconv.FormatInt(test.expires, 10), test.checkTime))
		})
	}
	require.Equal(t, http.StatusForbidden, rotated.checkAt("live/sample", "token", "invalid", now))
	require.Empty(t, original.previous)
	pruned := rotated.next("new", 30, 1121)
	require.Empty(t, pruned.previous)
}

func TestSignatureTTLReductionPreservesIssuedLinks(t *testing.T) {
	set := NewSignatureSet("same", 120).next("same", 20, 1000)
	token := AntiLeechSign("live/sample", "same", 1100)
	require.Equal(t, http.StatusOK, set.checkAt("live/sample", token, "1100", 1005))
	require.Equal(t, http.StatusGone, set.checkAt("live/sample", token, "1100", 1101))
	require.Equal(t, http.StatusForbidden, set.checkAt("other/sample", token, "1100", 1005))
	require.Equal(t, http.StatusOK, set.next("", 0, 1005).checkAt("live/sample", "", "", 1005))
}

func TestSignatureConcurrentRotation(t *testing.T) {
	set := NewSignatureSet("initial", 60)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for index := 0; index < 100; index++ {
			set.Update(strconv.Itoa(index), 60)
		}
	}()
	go func() {
		defer workers.Done()
		for index := 0; index < 100; index++ {
			if status := set.Check("live/sample", "forged", "1"); status != http.StatusForbidden {
				t.Errorf("forged token accepted during rotation: %d", status)
			}
		}
	}()
	workers.Wait()
}
