package config

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/redis/go-redis/v9"
)

type confd struct {
	prefix   string // e.g. /streamd/test/
	channels string // e.g. __keyspace@2__:/streamd/test/
	cache    map[string]string
	m        sync.RWMutex
}

func (c *confd) getValue(key string) string {
	c.m.RLock()
	defer c.m.RUnlock()
	str, ok := c.cache[key]
	if !ok {
		return ""
	}

	return str
}

func (f *Finder) transform(key string) string {
	if f.Separator == "/" {
		return key
	}

	return strings.ReplaceAll(key, "/", f.Separator)
}

func (f *Finder) clean(key string) string {
	k := key
	if f.Separator != "/" {
		k = strings.ReplaceAll(k, f.Separator, "/")
	}

	if !strings.HasPrefix(k, "/") {
		k = "/" + k
	}

	return k
}

func (f *Finder) makeCacheContext(ctx context.Context, key string) (map[string]string, error) {
	vars := make(map[string]string)

	key = f.clean(key)
	k := strings.ReplaceAll(key, "/*", "")
	k = strings.TrimSuffix(k, "/")
	k = f.transform(k)

	t, err := f.redisClient().Type(ctx, k).Result()
	if err != nil {
		return vars, err
	}

	if t == "string" {
		value, err := f.redisClient().Get(ctx, k).Result()
		if err == nil {
			vars[key] = value
		}
		if err != redis.Nil {
			return vars, err
		}
	} else if t == "hash" {
		idx := uint64(0)
		for {
			items, cur, err := f.redisClient().HScan(ctx, k, idx, "*", 1000).Result()
			if err != nil && err != redis.Nil {
				return vars, err
			}
			idx = cur

			for i := 0; i < len(items); i += 2 {
				vars[f.clean(k+"/"+items[i])] = items[i+1]
			}
			if idx == 0 {
				break
			}
		}
	} else {
		if key == "/" {
			k = "*"
		} else {
			k = fmt.Sprintf(f.transform("%s/*"), k)
		}

		idx := uint64(0)
		for {
			items, cur, err := f.redisClient().Scan(ctx, idx, k, 1000).Result()
			if err != nil && err != redis.Nil {
				return vars, err
			}

			idx = cur
			for _, item := range items {
				value, err := f.redisClient().Get(ctx, item).Result()
				if err == nil {
					vars[f.clean(item)] = value
				} else if !errors.Is(err, redis.Nil) {
					return nil, err
				}
			}
			if idx == 0 {
				break
			}
		}
	}

	return vars, nil
}

func (f *Finder) store(c *confd) error {
	return f.storeContext(f.runtime.ctx, c)
}

func (f *Finder) storeContext(ctx context.Context, c *confd) error {
	vars, err := f.makeCacheContext(ctx, c.prefix)
	if err != nil {
		if f.runtime.ctx.Err() == nil {
			log.Error("Redis refresh: ", err)
		}
		return err
	}

	c.m.Lock()
	c.cache = vars
	c.m.Unlock()
	return nil
}
