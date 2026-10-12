package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisFaultLogValidationRejectsUnrelatedErrors(test *testing.T) {
	require.Equal(test, 2, redisCommandCalls("# Commandstats\r\ncmdstat_del:calls=2,usec=5\r\n", "del"))
	require.Zero(test, redisCommandCalls("cmdstat_scan:calls=2,usec=5\r\n", "del"))
	require.Equal(test, -1, redisCommandCalls("cmdstat_del:calls=invalid\r\n", "del"))
	var log strings.Builder
	encoder := json.NewEncoder(&log)
	require.NoError(test, encoder.Encode(map[string]string{"level": "ERROR", "msg": "configuration reload rejected: prepare discovery: WRONGPASS invalid username-password pair"}))
	_, err := redisServerErrors([]byte(log.String()))
	require.ErrorContains(test, err, "missing explicit")
	require.NoError(test, encoder.Encode(map[string]string{"level": "ERROR", "msg": "Redis refresh: dial tcp 127.0.0.1:1234: connect: connection refused"}))
	expected, err := redisServerErrors([]byte(log.String()))
	require.NoError(test, err)
	require.Len(test, expected, 2)
	expected, err = redisServerErrors([]byte(log.String() + "redis: 2026/10/04 pool.go:762: redis: connection pool: failed to dial after 5 attempts: dial tcp 127.0.0.1:1234: connect: connection refused\n"))
	require.NoError(test, err)
	require.Len(test, expected, 3)
	for _, message := range []string{"subscriber EOF", "Redis media corruption EOF", "Redis refresh: invalid object checksum", "recording failed: connection refused"} {
		body, err := json.Marshal(map[string]string{"level": "ERROR", "msg": message})
		require.NoError(test, err)
		_, err = redisServerErrors([]byte(log.String() + string(body) + "\n"))
		require.ErrorContains(test, err, "unexpected service error")
	}
	_, err = redisServerErrors([]byte(log.String() + "redis: unexpected library failure\n"))
	require.ErrorContains(test, err, "unexpected non-JSON")
}
