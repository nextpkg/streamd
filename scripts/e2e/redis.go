package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nextpkg/streamd/config"
)

func (harness *harness) redisRegistry(container *redisContainer, label string) error {
	snapshot := make(map[string]any)
	err := harness.until("Redis registry "+label, 8*time.Second, func() (bool, error) {
		for _, app := range []string{"flv", "ts"} {
			key := "/streamd/" + app + "/sample"
			value, err := container.client.Get(harness.ctx, key).Result()
			if err != nil {
				return false, nil
			}
			ttl, err := container.client.PTTL(harness.ctx, key).Result()
			if err != nil {
				return false, err
			}
			if value != "127.0.0.1" || ttl <= 0 || ttl > 5*time.Second {
				return false, nil
			}
			snapshot[app] = map[string]any{"key": key, "advice": value, "ttl_milliseconds": ttl.Milliseconds()}
		}
		return true, nil
	})
	if err == nil {
		harness.results["redis_registry_"+label] = snapshot
	}
	return err
}

func (harness *harness) redisDiscovery() (result error) {
	primary, err := harness.startRedisContainer("primary")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, primary.close(harness)) }()
	replacement, err := harness.startRedisContainer("replacement")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, replacement.close(harness)) }()
	defer harness.cleanup()
	if err := harness.redisContract(primary); err != nil {
		return err
	}
	if err := harness.until("contract discovery subscriptions closed", 3*time.Second, func() (bool, error) {
		count, err := primary.client.PubSubNumPat(harness.ctx).Result()
		return count == 0, err
	}); err != nil {
		return err
	}
	harness.results["redis_contract_subscriptions_closed"] = true
	harness.recordingApplications("immediate", 4, 2)
	harness.settings.Finder = &config.Finder{Advice: "127.0.0.1", Expire: 5, Interval: 1, EnableRelay: true, Separator: "/",
		Redis: &config.RedisConfig{Address: primary.address, Password: redisTestPassword, DB: 13}}
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
	if err := harness.redisRegistry(primary, "initial"); err != nil {
		return err
	}
	if err := harness.sleep(6 * time.Second); err != nil {
		return err
	}
	if err := harness.redisRegistry(primary, "renewed_beyond_original_ttl"); err != nil {
		return err
	}
	harness.settings.Finder.Redis.Password = "intentional-wrong-password"
	if err := harness.atomicSettings(); err != nil {
		return err
	}
	if err := harness.until("wrong Redis credentials rejected", 6*time.Second, func() (bool, error) {
		body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
		return bytes.Contains(body, []byte("configuration reload rejected")) && bytes.Contains(body, []byte("WRONGPASS")), err
	}); err != nil {
		return err
	}
	report, err := harness.configuration()
	if err != nil || report.Revision != 1 {
		return fmt.Errorf("bad Redis candidate replaced live configuration: %+v: %v", report, err)
	}
	harness.results["redis_bad_candidate_retained_revision"] = report.Revision
	harness.settings.Finder.Redis.Password = redisTestPassword
	if err := harness.atomicSettings(); err != nil {
		return err
	}
	if err := harness.redisRegistry(primary, "after_rejected_candidate"); err != nil {
		return err
	}
	commandContext, cancel := context.WithTimeout(harness.ctx, 25*time.Second)
	defer cancel()
	if _, err := primary.runner.command(commandContext, "stop", "--time", "1", primary.runner.name); err != nil {
		return err
	}
	harness.results["redis_outage_started_seconds"] = time.Since(harness.started).Seconds()
	if err := primary.client.Ping(harness.ctx).Err(); err == nil {
		return errors.New("Redis outage was not enforced")
	}
	if err := harness.sleep(6 * time.Second); err != nil {
		return err
	}
	if _, err := primary.runner.command(commandContext, "start", primary.runner.name); err != nil {
		return err
	}
	binding, err := primary.runner.command(commandContext, "port", primary.runner.name, "6379/tcp")
	if err != nil || strings.TrimSpace(string(binding)) != primary.address {
		return fmt.Errorf("Redis restart changed the tested endpoint: %s: %v", binding, err)
	}
	if err := harness.until("Redis restart readiness at the original endpoint", 10*time.Second, func() (bool, error) { return primary.client.Ping(harness.ctx).Err() == nil, nil }); err != nil {
		return err
	}
	info, err := primary.client.Info(harness.ctx, "server").Result()
	if err != nil {
		return err
	}
	previousInfo := harness.results["redis_container_primary"].(map[string]any)["server_info"].(string)
	previousID, restartedID := redisRunID(previousInfo), redisRunID(info)
	if previousID == "" || restartedID == "" || previousID == restartedID {
		return errors.New("Redis process restart was not independently confirmed")
	}
	harness.results["redis_restart"] = map[string]any{"address_unchanged": true, "previous_run_id": previousID, "restarted_run_id": restartedID}
	if err := harness.redisRegistry(primary, "reregistered_after_restart"); err != nil {
		return err
	}
	harness.results["redis_recovered_seconds"] = time.Since(harness.started).Seconds()
	harness.settings.Finder.Redis.Address = replacement.address
	if err := harness.commit("Redis endpoint switch", map[componentKey]string{{"recording", "flv/sample"}: "active", {"recording", "ts/sample"}: "active"}); err != nil {
		return err
	}
	if err := harness.redisRegistry(replacement, "replacement_endpoint"); err != nil {
		return err
	}
	if err := harness.until("old Redis registrations expire and subscriptions close", 7*time.Second, func() (bool, error) {
		keys, err := primary.client.Exists(harness.ctx, "/streamd/flv/sample", "/streamd/ts/sample").Result()
		if err != nil {
			return false, err
		}
		patterns, err := primary.client.PubSubNumPat(harness.ctx).Result()
		return keys == 0 && patterns == 0, err
	}); err != nil {
		return err
	}
	harness.results["redis_old_endpoint_retired"] = true
	for _, app := range []string{"flv", "ts"} {
		state, err := harness.recordingStatus(app)
		if err != nil || state.State != "active" || state.Revision != 2 || state.Failures != 0 {
			return fmt.Errorf("Redis transition affected recording: %+v: %v", state, err)
		}
	}
	harness.results["redis_recorders_healthy"] = true
	beforeDisconnect, err := replacement.client.Info(harness.ctx, "commandstats").Result()
	if err != nil {
		return err
	}
	if err := harness.finishPlayers(players, 46*25*9/10); err != nil {
		return err
	}
	if err := harness.finishPublishers(publishers, "publish-", 75*time.Second); err != nil {
		return err
	}
	if err := harness.until("natural disconnect deregistration", 4*time.Second, func() (bool, error) {
		count, err := replacement.client.Exists(harness.ctx, "/streamd/flv/sample", "/streamd/ts/sample").Result()
		return count == 0, err
	}); err != nil {
		return err
	}
	afterDisconnect, err := replacement.client.Info(harness.ctx, "commandstats").Result()
	if err != nil {
		return err
	}
	deletions := redisCommandCalls(afterDisconnect, "del") - redisCommandCalls(beforeDisconnect, "del")
	if deletions != 2 {
		return fmt.Errorf("explicit publisher deregistration unverified: got %d DEL calls, want 2", deletions)
	}
	harness.results["redis_disconnect_del_calls"] = deletions
	harness.results["redis_publishers_deregistered"] = true
	if err := harness.stopServer(); err != nil {
		return err
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		count, err := replacement.client.PubSubNumPat(harness.ctx).Result()
		if err != nil {
			return err
		}
		if count == 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("replacement Redis still has %d discovery subscriptions after shutdown", count)
		}
		if err := harness.sleep(20 * time.Millisecond); err != nil {
			return err
		}
	}
	harness.results["redis_shutdown_subscriptions_closed"] = true
	if err := harness.recordings("recordings", 28, true); err != nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(harness.evidence, "server.log"))
	if err != nil {
		return err
	}
	expected, err := redisServerErrors(body)
	harness.results["expected_server_errors"] = expected
	if err != nil {
		return err
	}
	return harness.checkLogs("publish-*.log", "play-*.log")
}

func redisServerErrors(body []byte) ([]string, error) {
	var expected []string
	wrongPassword, outage := false, false
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		var event struct{ Level, Msg string }
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			pubsubClosed := strings.Contains(line, "pubsub.go:") && strings.Contains(line, "discarding bad PubSub connection") && strings.Contains(line, "EOF")
			poolRefused := strings.Contains(line, "pool.go:") && strings.Contains(line, "redis: connection pool: failed to dial after") && strings.Contains(line, "connection refused")
			if strings.HasPrefix(line, "redis: ") && (pubsubClosed || poolRefused) {
				expected = append(expected, line)
				continue
			}
			return expected, fmt.Errorf("unexpected non-JSON service log: %s", line)
		}
		if event.Level != "ERROR" {
			continue
		}
		if strings.HasPrefix(event.Msg, "configuration reload rejected: prepare discovery:") && strings.Contains(event.Msg, "WRONGPASS") {
			wrongPassword = true
		} else {
			redisOperation := false
			for _, prefix := range []string{"Redis refresh: ", "Redis keepalive: ", "Redis register: ", "Redis notify: ", "Redis deregister: ", "[flv/sample]Find Advice failed.Err=", "[ts/sample]Find Advice failed.Err="} {
				redisOperation = redisOperation || strings.HasPrefix(event.Msg, prefix)
			}
			networkFailure := strings.Contains(event.Msg, "connection refused") || strings.Contains(event.Msg, "connection reset by peer") || strings.Contains(event.Msg, "EOF") || strings.Contains(event.Msg, "context canceled")
			if !redisOperation || !networkFailure {
				return expected, fmt.Errorf("unexpected service error: %s", event.Msg)
			}
			outage = outage || !strings.Contains(event.Msg, "context canceled")
		}
		expected = append(expected, event.Msg)
	}
	if err := scanner.Err(); err != nil {
		return expected, err
	}
	if !wrongPassword || !outage {
		return expected, errors.New("missing explicit Redis credential/outage diagnostics")
	}
	return expected, nil
}

func redisRunID(info string) string {
	for _, line := range strings.Split(info, "\n") {
		if value, exists := strings.CutPrefix(line, "run_id:"); exists {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func redisCommandCalls(info, command string) int {
	for _, line := range strings.Split(info, "\n") {
		if value, exists := strings.CutPrefix(line, "cmdstat_"+command+":"); exists {
			var count int
			if _, err := fmt.Sscanf(value, "calls=%d,", &count); err != nil {
				return -1
			}
			return count
		}
	}
	return 0
}
