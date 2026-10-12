package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRedisPreparationEvidenceRequiresEveryOwnedRejection(test *testing.T) {
	var lines []string
	for _, command := range []string{"set", "get", "expire", "del", "publish", "psubscribe"} {
		body, _ := json.Marshal(map[string]string{"Level": "ERROR", "Msg": "configuration reload rejected: prepare discovery: NOPERM no permissions for '" + command + "' command"})
		lines = append(lines, string(body))
	}
	valid := strings.Join(lines, "\n")
	if err := redisPreparationErrors(valid); err != nil {
		test.Fatal(err)
	}
	for _, invalid := range []string{"", strings.Join(lines[1:], "\n"), valid + "\n" + lines[0], strings.Replace(valid, "ERROR", "WARN", 1), strings.Replace(valid, "NOPERM", "connection lost", 1), strings.Replace(valid, "prepare discovery", "unexpected upload", 1), valid + "\n" + "WARNING: DATA RACE"} {
		if err := redisPreparationErrors(invalid); err == nil {
			test.Fatal("incomplete or unrelated failure evidence accepted")
		}
	}
}

func TestRedisSubscriptionEvidenceCountsConnectionsByIdentity(test *testing.T) {
	owners, err := redisSubscriptionOwners("id=1 flags=P user=default\nid=2 flags=P user=default\nid=3 flags=P user=candidate\nid=4 flags=N user=default\n")
	if err != nil || owners["default"] != 2 || owners["candidate"] != 1 || len(owners) != 2 {
		test.Fatalf("duplicate subscription patterns hid a connection: %v %v", owners, err)
	}
	if _, err := redisSubscriptionOwners("id=1 flags=P"); err == nil {
		test.Fatal("ownerless subscription accepted")
	}
}
