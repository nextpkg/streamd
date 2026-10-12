package config

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

func (finder *Finder) prepareWrites(ctx context.Context, cache *confd) (result error) {
	key := cache.prefix + "__streamd_probe_" + rand.Text()
	created, err := finder.redisClient().SetNX(ctx, key, "", 5*time.Second).Result()
	if err != nil {
		return fmt.Errorf("Redis registration probe: %w", err)
	}
	if !created {
		return errors.New("Redis registration probe key already exists")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := finder.redisClient().Del(cleanup, key).Err(); err != nil {
			result = errors.Join(result, fmt.Errorf("Redis registration probe cleanup (expires within five seconds): %w", err))
		}
	}()
	value, err := finder.redisClient().Get(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("Redis registration probe read: %w", err)
	}
	if value != "" {
		return errors.New("Redis registration probe contents changed")
	}
	extended, err := finder.redisClient().Expire(ctx, key, 5*time.Second).Result()
	if err != nil {
		return fmt.Errorf("Redis registration probe renewal: %w", err)
	}
	if !extended {
		return errors.New("Redis registration probe disappeared before renewal")
	}
	if err := finder.redisClient().Publish(ctx, cache.channels, "set").Err(); err != nil {
		return fmt.Errorf("Redis registration notification probe: %w", err)
	}
	return nil
}
