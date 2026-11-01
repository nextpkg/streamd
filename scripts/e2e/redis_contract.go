package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nextpkg/streamd/config"
	"github.com/redis/go-redis/v9"
)

func (harness *harness) redisFinder(container *redisContainer, app, separator string, interval time.Duration, username string) (*config.Config, error) {
	settings := config.NewConfig()
	settings.App[0].AppName = app
	settings.Finder = config.Finder{Advice: "127.0.0.2", Expire: interval*3 + 2, Interval: interval, Separator: separator,
		Redis: &config.RedisConfig{Address: container.address, Username: username, Password: redisTestPassword, DB: 13}}
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	if err := settings.InitFinder(harness.ctx); err != nil {
		settings.Revoke()
		return nil, err
	}
	return settings, nil
}

func (harness *harness) redisContract(container *redisContainer) (result error) {
	client := container.client
	if err := harness.redisCacheLayouts(container); err != nil {
		return err
	}
	if err := client.ACLSetUser(harness.ctx, "observer", "on", ">"+redisTestPassword, "~*", "&*", "+@all").Err(); err != nil {
		return err
	}
	polling, err := harness.redisFinder(container, "probe", "/", 1, "observer")
	if err != nil {
		return err
	}
	defer polling.Revoke()
	custom, err := harness.redisFinder(container, "separator", "#", 1, "default")
	if err != nil {
		return err
	}
	defer custom.Revoke()
	notified, err := harness.redisFinder(container, "notify", "/", 30, "default")
	if err != nil {
		return err
	}
	defer notified.Revoke()
	if err := harness.until("three production discovery subscriptions", 3*time.Second, func() (bool, error) {
		count, err := client.PubSubNumPat(harness.ctx).Result()
		return count == 3, err
	}); err != nil {
		return err
	}
	for _, probe := range []struct {
		settings *config.Config
		app, key string
	}{{polling, "probe", "/streamd/probe/nested/channel"}, {custom, "separator", "#streamd#separator#nested/channel"}} {
		probe.settings.Finder.Register(probe.app, "nested/channel")
		if err := harness.until("registered Redis route "+probe.app, 3*time.Second, func() (bool, error) {
			value, err := client.Get(harness.ctx, probe.key).Result()
			return value == "127.0.0.2" && probe.settings.Finder.GetBackend(probe.app, "nested/channel") == value, err
		}); err != nil {
			return err
		}
		if err := client.Expire(harness.ctx, probe.key, time.Second).Err(); err != nil {
			return err
		}
		if !probe.settings.Finder.Keepalive(probe.app, "nested/channel") {
			return errors.New("redis renewal failed")
		}
		ttl, err := client.PTTL(harness.ctx, probe.key).Result()
		if err != nil || ttl < 4*time.Second || ttl > 5*time.Second {
			return fmt.Errorf("unexpected renewed TTL %v: %v", ttl, err)
		}
		probe.settings.Finder.Deregister(probe.app, "nested/channel")
		if err := harness.until("Redis deregistration "+probe.app, 3*time.Second, func() (bool, error) {
			_, err := client.Get(harness.ctx, probe.key).Result()
			return errors.Is(err, redis.Nil) && probe.settings.Finder.GetBackend(probe.app, "nested/channel") == "", nil
		}); err != nil {
			return err
		}
		if probe.settings.Finder.Keepalive(probe.app, "nested/channel") {
			return errors.New("missing Redis key was renewed")
		}
	}
	started := time.Now()
	for _, value := range []string{"first-peer", "replacement-peer"} {
		if err := client.Set(harness.ctx, "/streamd/notify/external", value, 2*time.Second).Err(); err != nil {
			return err
		}
		if err := harness.until("keyspace notification before thirty-second polling interval", time.Second, func() (bool, error) {
			return notified.Finder.GetBackend("notify", "external") == value, nil
		}); err != nil {
			return err
		}
	}
	if err := harness.until("keyspace expiration notification", 4*time.Second, func() (bool, error) { return notified.Finder.GetBackend("notify", "external") == "", nil }); err != nil {
		return err
	}
	harness.results["redis_notifications_seconds"] = time.Since(started).Seconds()
	if err := client.ConfigSet(harness.ctx, "notify-keyspace-events", "").Err(); err != nil {
		return err
	}
	defer func() {
		result = errors.Join(result, client.ConfigSet(harness.ctx, "notify-keyspace-events", "KEA").Err())
	}()
	if err := client.Set(harness.ctx, "/streamd/probe/polled", "confirmed-peer", 0).Err(); err != nil {
		return err
	}
	if err := harness.until("polling without notifications", 3*time.Second, func() (bool, error) { return polling.Finder.GetBackend("probe", "polled") == "confirmed-peer", nil }); err != nil {
		return err
	}
	if err := client.ACLSetUser(harness.ctx, "observer", "-get").Err(); err != nil {
		return err
	}
	defer func() { result = errors.Join(result, client.ACLSetUser(harness.ctx, "observer", "+get").Err()) }()
	_, denied := polling.Finder.FindAdvice("probe", "polled")
	if denied == nil || !strings.Contains(denied.Error(), "NOPERM") {
		return fmt.Errorf("redis ACL denial not exercised: %v", denied)
	}
	if err := harness.sleep(1500 * time.Millisecond); err != nil {
		return err
	}
	if polling.Finder.GetBackend("probe", "polled") != "confirmed-peer" {
		return errors.New("redis read failure erased the confirmed discovery cache")
	}
	rejected, err := harness.redisFinder(container, "probe", "/", 1, "observer")
	if rejected != nil {
		rejected.Revoke()
	}
	if err == nil || !strings.Contains(err.Error(), "NOPERM") {
		return fmt.Errorf("unreadable Redis candidate was not rejected: %v", err)
	}
	if err := client.ACLSetUser(harness.ctx, "observer", "+get").Err(); err != nil {
		return err
	}
	if err := client.Set(harness.ctx, "/streamd/probe/polled", "recovered-peer", 0).Err(); err != nil {
		return err
	}
	if err := harness.until("cache recovery after ACL restoration", 3*time.Second, func() (bool, error) { return polling.Finder.GetBackend("probe", "polled") == "recovered-peer", nil }); err != nil {
		return err
	}
	harness.results["redis_contract"] = map[string]any{"registered": true, "renewed": true, "deregistered": true, "missing_renewal_rejected": true, "custom_separator_nested_key": true, "pubsub_before_poll": true, "expiration_notification": true, "polling_without_notifications": true, "read_failure_retained_snapshot": true, "unreadable_candidate_rejected": true, "acl_recovery": true, "db": 13}
	return nil
}

func (harness *harness) redisCacheLayouts(container *redisContainer) error {
	client := container.client
	if err := client.Set(harness.ctx, "/streamd/direct", "direct-peer", 0).Err(); err != nil {
		return err
	}
	if err := client.HSet(harness.ctx, "#streamd#hash", "nested/channel", "hash-peer").Err(); err != nil {
		return err
	}
	for _, layout := range []struct{ app, separator, channel, expected string }{{"direct", "/", "", "direct-peer"}, {"hash", "#", "nested/channel", "hash-peer"}} {
		settings, err := harness.redisFinder(container, layout.app, layout.separator, 30, "default")
		if err != nil {
			return err
		}
		actual := settings.Finder.GetBackend(layout.app, layout.channel)
		settings.Revoke()
		if actual != layout.expected {
			return fmt.Errorf("redis %s cache layout: got %q want %q", layout.app, actual, layout.expected)
		}
	}
	pipeline := client.Pipeline()
	for index := 0; index < 1500; index++ {
		pipeline.Set(harness.ctx, fmt.Sprintf("/streamd/pages/channel-%04d", index), "paged-peer", 0)
	}
	if _, err := pipeline.Exec(harness.ctx); err != nil {
		return err
	}
	before, err := client.Info(harness.ctx, "commandstats").Result()
	if err != nil {
		return err
	}
	settings, err := harness.redisFinder(container, "pages", "/", 30, "default")
	if err != nil {
		return err
	}
	defer settings.Revoke()
	for index := 0; index < 1500; index++ {
		if settings.Finder.GetBackend("pages", fmt.Sprintf("channel-%04d", index)) != "paged-peer" {
			return fmt.Errorf("redis SCAN lost channel %d", index)
		}
	}
	after, err := client.Info(harness.ctx, "commandstats").Result()
	if err != nil {
		return err
	}
	readCalls := func(info string) int {
		for _, line := range strings.Split(info, "\n") {
			if strings.HasPrefix(line, "cmdstat_scan:") {
				var count int
				_, _ = fmt.Sscanf(line, "cmdstat_scan:calls=%d,", &count)
				return count
			}
		}
		return 0
	}
	calls := readCalls(after) - readCalls(before)
	if calls < 2 {
		return fmt.Errorf("redis SCAN pagination not exercised: %d calls", calls)
	}
	harness.results["redis_cache_layouts"] = map[string]any{"string_root": true, "hash_custom_separator": true, "paged_key_count": 1500, "scan_calls": calls}
	return nil
}
