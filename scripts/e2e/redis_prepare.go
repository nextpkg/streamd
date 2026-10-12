package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/nextpkg/streamd/config"
)

func (harness *harness) redisPrepareChecks(container *redisContainer) error {
	results := map[string]bool{}
	var failures []error
	for _, command := range []string{"set", "get", "expire", "del", "publish", "psubscribe"} {
		if err := container.client.ACLSetUser(harness.ctx, "candidate", "reset", "on", ">"+redisTestPassword, "~*", "&*", "+@all", "-"+command).Err(); err != nil {
			return err
		}
		candidate := config.NewConfig()
		candidate.Finder = config.Finder{Advice: "127.0.0.1", Expire: 5, Interval: 1, Separator: "/", Redis: &config.RedisConfig{Address: container.address, Username: "candidate", Password: redisTestPassword, DB: 13}}
		if err := candidate.Validate(); err != nil {
			return err
		}
		err := candidate.InitFinder(harness.ctx)
		candidate.Revoke()
		results[command] = err != nil && strings.Contains(err.Error(), "NOPERM")
		if !results[command] {
			failures = append(failures, fmt.Errorf("Redis candidate without %s permission accepted or wrong error: %v", command, err))
		}
	}
	harness.results["redis_prepare_contract"] = results
	if err := harness.until("all owned preparation probes expire or delete", 6*time.Second, func() (bool, error) {
		keys, err := container.client.Keys(harness.ctx, "*__streamd_probe_*").Result()
		return len(keys) == 0, err
	}); err != nil {
		failures = append(failures, err)
	}
	harness.results["redis_prepare_probes_absent"] = len(failures) == 0
	return errors.Join(failures...)
}

func (harness *harness) redisPrepareMedia() (result error) {
	container, err := harness.startRedisContainer("prepare")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, container.close(harness)) }()
	defer harness.cleanup()
	if err := harness.redisPrepareChecks(container); err != nil {
		return err
	}
	harness.recordingApplications("immediate", 4, 2)
	harness.settings.Finder = &config.Finder{Advice: "127.0.0.1", Expire: 5, Interval: 1, Separator: "/", Redis: &config.RedisConfig{Address: container.address, Password: redisTestPassword, DB: 13}}
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	players, watch, err := harness.faultPlayers()
	if watch != nil {
		defer func() { result = errors.Join(result, watch.finish(harness)) }()
	}
	if err != nil {
		return err
	}
	if err := harness.redisRegistry(container, "before-candidates"); err != nil {
		return err
	}
	harness.settings.Finder.Redis.Username = "candidate"
	for _, command := range []string{"set", "get", "expire", "del", "publish", "psubscribe"} {
		if err := container.client.ACLSetUser(harness.ctx, "candidate", "reset", "on", ">"+redisTestPassword, "~*", "&*", "+@all", "-"+command).Err(); err != nil {
			return err
		}
		if err := harness.atomicSettings(); err != nil {
			return err
		}
		if err := harness.until("rejected Redis "+command+" candidate", 6*time.Second, func() (bool, error) {
			body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
			return strings.Contains(string(body), "NOPERM") && strings.Contains(string(body), "'"+command+"' command"), err
		}); err != nil {
			return err
		}
		report, err := harness.configuration()
		if err != nil || report.Revision != 1 {
			return fmt.Errorf("Redis %s candidate replaced old configuration: %+v: %v", command, report, err)
		}
		if err := harness.redisRegistry(container, "rejected-"+command); err != nil {
			return err
		}
		if err := harness.waitRedisSubscriptions(container, map[string]int{"default": 2}); err != nil {
			return err
		}
	}
	if err := container.client.ACLSetUser(harness.ctx, "candidate", "+@all").Err(); err != nil {
		return err
	}
	if err := harness.commit("prepared-Redis-credentials", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
		return err
	}
	if err := harness.waitRedisSubscriptions(container, map[string]int{"candidate": 2}); err != nil {
		return err
	}
	if err := harness.sleep(6 * time.Second); err != nil {
		return err
	}
	if err := harness.redisRegistry(container, "new-credentials-renewed"); err != nil {
		return err
	}
	if err := harness.finishPlayers(players, 46*25*9/10); err != nil {
		return err
	}
	for app, child := range publishers {
		if err := harness.finish("publish-"+app, child, 20*time.Second); err != nil {
			return err
		}
	}
	if err := harness.until("registrations and probes removed", 4*time.Second, func() (bool, error) {
		keys, err := container.client.Keys(harness.ctx, "/streamd/*").Result()
		return len(keys) == 0, err
	}); err != nil {
		return err
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	if err := harness.waitRedisSubscriptions(container, map[string]int{}); err != nil {
		return err
	}
	if err := harness.recordings("recordings", 28, true); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	if err := redisPreparationErrors(string(body)); err != nil {
		return err
	}
	harness.results["redis_preparation_live_verified"] = true
	harness.results["redis_preparation_final_resources"] = map[string]int{"keys": 0, "subscriptions": 0}
	return harness.checkLogs("publish-*.log", "play-*.log")
}

func redisSubscriptionOwners(body string) (map[string]int, error) {
	owners := map[string]int{}
	for _, line := range strings.Split(body, "\n") {
		fields := map[string]string{}
		for _, field := range strings.Fields(line) {
			name, value, found := strings.Cut(field, "=")
			if found {
				fields[name] = value
			}
		}
		if !strings.Contains(fields["flags"], "P") {
			continue
		}
		if fields["user"] == "" {
			return nil, errors.New("subscription owner missing from Redis client list")
		}
		owners[fields["user"]]++
	}
	return owners, nil
}

func (harness *harness) waitRedisSubscriptions(container *redisContainer, expected map[string]int) error {
	deadline := time.Now().Add(3 * time.Second)
	var owners map[string]int
	for time.Now().Before(deadline) {
		body, err := container.client.ClientList(harness.ctx).Result()
		if err != nil {
			return err
		}
		owners, err = redisSubscriptionOwners(body)
		if err != nil {
			return err
		}
		if reflect.DeepEqual(owners, expected) {
			return nil
		}
		if err := harness.sleep(20 * time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("Redis subscription owners: got %v want %v", owners, expected)
}

func redisPreparationErrors(body string) error {
	denied := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		var event struct{ Level, Msg string }
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return err
		}
		if event.Level != "WARN" && event.Level != "ERROR" {
			continue
		}
		if event.Level != "ERROR" || !strings.HasPrefix(event.Msg, "configuration reload rejected: prepare discovery:") || !strings.Contains(event.Msg, "NOPERM") {
			return fmt.Errorf("unexpected service event: %s", line)
		}
		matched := false
		for _, command := range []string{"set", "get", "expire", "del", "publish", "psubscribe"} {
			if strings.Contains(event.Msg, "'"+command+"' command") {
				denied[command]++
				matched = true
			}
		}
		if !matched {
			return errors.New("unclassified Redis preparation failure")
		}
	}
	if len(denied) != 6 {
		return errors.New("missing Redis permission rejection")
	}
	for _, count := range denied {
		if count != 1 {
			return errors.New("duplicate Redis permission rejection")
		}
	}
	return nil
}
