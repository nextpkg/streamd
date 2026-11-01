package config

import (
	"context"
	"errors"
	"maps"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nextpkg/streamd/internal/logging"
	"github.com/redis/go-redis/v9"
)

type RedisConfig struct {
	Address  string `json:"address"`
	Username string `json:"username"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

type Finder struct {
	Advice      string        `json:"advice"`
	HTTPAdvice  string        `json:"http_advice,omitempty"`
	Expire      time.Duration `json:"expire"`
	Interval    time.Duration `json:"interval"`
	EnableRelay bool          `json:"enable_relay"`
	Separator   string        `json:"separator"`
	Redis       *RedisConfig  `json:"redis"`
	runtime     *finderRuntime
}

// finderRuntime owns the resources of one prepared discovery generation.
type finderRuntime struct {
	ctx    context.Context
	cancel context.CancelFunc
	confd  map[string]*confd
	client *redis.Client
	advice string
	wg     sync.WaitGroup
	once   sync.Once
}

func GetFinder() *Finder { return &Get().Finder }

func GetLocalRoutes(address string) (string, error) {
	if _, _, err := net.SplitHostPort(address); err != nil {
		address = net.JoinHostPort(strings.Trim(address, "[]"), "7")
	}
	connection, err := net.DialTimeout("udp", address, time.Second)
	if err != nil {
		return "", err
	}
	defer func() { _ = connection.Close() }()
	host, _, err := net.SplitHostPort(connection.LocalAddr().String())
	return host, err
}

func (finder *Finder) check() error {
	if finder.HTTPAdvice != "" {
		if _, err := discoveryAuthority(finder.HTTPAdvice); err != nil {
			return err
		}
		if finder.Advice != "" {
			if _, err := discoveryAuthority(finder.Advice); err != nil {
				return err
			}
		}
	}
	if finder.Interval <= 0 || finder.Expire <= finder.Interval {
		return errors.New("finder requires 0 < interval < expire")
	}
	if finder.Separator == "" {
		finder.Separator = "/"
	}
	if finder.Redis != nil {
		if _, _, err := net.SplitHostPort(finder.Redis.Address); err != nil {
			return errors.New("invalid Redis address")
		}
	}
	return nil
}

func (finder *Finder) init(ctx context.Context, apps map[string]*App) error {
	if err := finder.check(); err != nil {
		return err
	}
	finder.runtime = &finderRuntime{confd: make(map[string]*confd)}
	for name := range apps {
		database := "0"
		if finder.Redis != nil {
			database = strconv.Itoa(finder.Redis.DB)
		}
		prefix := finder.transform("/streamd/" + name + "/")
		finder.runtime.confd[name] = &confd{prefix: prefix, channels: "__keyspace@" + database + "__:" + prefix, cache: make(map[string]string)}
	}
	finder.runtime.ctx, finder.runtime.cancel = context.WithCancel(ctx)
	return nil
}

func (finder *Finder) validate(ctx context.Context, apps map[string]*App) error {
	if err := finder.init(ctx, apps); err != nil {
		return err
	}
	if finder.Redis == nil {
		if finder.HTTPAdvice != "" {
			advice := finder.Advice
			if advice == "" {
				advice = "127.0.0.1"
			}
			var err error
			finder.runtime.advice, err = discoveryAdvertisement(advice, finder.HTTPAdvice)
			return err
		}
		return nil
	}
	client := redis.NewClient(&redis.Options{Addr: finder.Redis.Address, Username: finder.Redis.Username, Password: finder.Redis.Password, DB: finder.Redis.DB, DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, MaxRetries: 1, ContextTimeoutEnabled: true})
	finder.runtime.client = client
	probe, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := client.Ping(probe).Err(); err != nil {
		finder.Close()
		return err
	}
	finder.runtime.advice = finder.Advice
	if finder.runtime.advice == "" {
		advice, err := GetLocalRoutes(finder.Redis.Address)
		if err != nil {
			finder.Close()
			return err
		}
		finder.runtime.advice = advice
	}
	advertisement, err := discoveryAdvertisement(finder.runtime.advice, finder.HTTPAdvice)
	if err != nil {
		finder.Close()
		return err
	}
	finder.runtime.advice = advertisement
	for _, cache := range finder.runtime.confd {
		if err := finder.prepareWrites(probe, cache); err != nil {
			finder.Close()
			return err
		}
		if err := finder.storeContext(probe, cache); err != nil {
			finder.Close()
			return err
		}
		subscription := finder.redisClient().PSubscribe(probe)
		if err := subscription.PSubscribe(probe, cache.channels+"*"); err != nil {
			_ = subscription.Close()
			finder.Close()
			return err
		}
		confirmation, err := subscription.Receive(probe)
		if err != nil {
			_ = subscription.Close()
			finder.Close()
			return err
		}
		ready, valid := confirmation.(*redis.Subscription)
		if !valid || ready.Kind != "psubscribe" || ready.Channel != cache.channels+"*" || ready.Count != 1 {
			_ = subscription.Close()
			finder.Close()
			return errors.New("redis discovery subscription was not confirmed")
		}
		finder.runtime.wg.Add(1)
		go finder.monitor(cache, subscription)
	}
	return nil
}

func (finder *Finder) monitor(cache *confd, subscription *redis.PubSub) {
	defer finder.runtime.wg.Done()
	defer func() { _ = subscription.Close() }()
	ticker := time.NewTicker(finder.Interval * time.Second)
	defer ticker.Stop()
	messages := subscription.Channel()
	for {
		select {
		case <-finder.runtime.ctx.Done():
			return
		case _, ok := <-messages:
			if !ok {
				return
			}
			_ = finder.store(cache) // store logs failures and retains the last confirmed cache for retry.
		case <-ticker.C:
			_ = finder.store(cache) // store logs failures and retains the last confirmed cache for retry.
		}
	}
}

func (finder *Finder) Close() {
	if finder.runtime == nil {
		return
	}
	finder.runtime.once.Do(func() {
		finder.runtime.cancel()
		finder.runtime.wg.Wait()
		if finder.runtime.client != nil {
			_ = finder.runtime.client.Close()
		}
	})
}

func (finder *Finder) redisClient() *redis.Client { return finder.runtime.client }
func (finder *Finder) GetAdvice() string {
	if finder.runtime == nil {
		return ""
	}
	return finder.runtime.advice
}

func (finder *Finder) GetBackend(app, instance string) string {
	if cache := finder.cacheFor(app); cache != nil {
		return cache.getValue(finder.clean(cache.prefix + instance))
	}
	return ""
}

func (finder *Finder) Keepalive(app, instance string) bool {
	if finder.Redis == nil {
		return false
	}
	cache := finder.cacheFor(app)
	if cache == nil {
		return false
	}
	extended, err := finder.redisClient().Expire(finder.runtime.ctx, cache.prefix+instance, finder.Expire*time.Second).Result()
	if err != nil {
		logging.Error("Redis keepalive: ", err)
	}
	return extended && err == nil
}

func (finder *Finder) Register(app, instance string) {
	cache := finder.cacheFor(app)
	if cache == nil {
		return
	}
	if finder.Redis == nil {
		cache.m.Lock()
		advice := "127.0.0.1"
		if finder.HTTPAdvice != "" {
			advice = finder.GetAdvice()
		}
		cache.cache[finder.clean(cache.prefix+instance)] = advice
		cache.m.Unlock()
		return
	}
	if err := finder.redisClient().Set(finder.runtime.ctx, cache.prefix+instance, finder.GetAdvice(), finder.Expire*time.Second).Err(); err != nil {
		logging.Error("Redis register: ", err)
		return
	}
	if err := finder.redisClient().Publish(finder.runtime.ctx, cache.channels, "set").Err(); err != nil {
		logging.Error("Redis notify: ", err)
	}
}

func (finder *Finder) Deregister(app, instance string) {
	cache := finder.cacheFor(app)
	if cache == nil {
		return
	}
	if finder.Redis == nil {
		cache.m.Lock()
		delete(cache.cache, finder.clean(cache.prefix+instance))
		cache.m.Unlock()
		return
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(finder.runtime.ctx), 2*time.Second)
	defer cancel()
	if err := finder.redisClient().Del(cleanup, cache.prefix+instance).Err(); err != nil {
		logging.Error("Redis deregister: ", err)
		return
	}
	if err := finder.redisClient().Publish(cleanup, cache.channels, "del").Err(); err != nil {
		logging.Error("Redis notify: ", err)
	}
}

func (finder *Finder) FindAdvice(app, instance string) (string, error) {
	if finder.Redis == nil {
		return "", nil
	}
	cache := finder.cacheFor(app)
	if cache == nil {
		return "", nil
	}
	advice, err := finder.redisClient().Get(finder.runtime.ctx, cache.prefix+instance).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return advice, err
}

// cacheFor preserves empty lookup behavior before discovery is initialized.
func (finder *Finder) cacheFor(app string) *confd {
	if finder.runtime == nil {
		return nil
	}
	return finder.runtime.confd[app]
}

// inheritLocalRegistrations prepares an unpublished generation. Redis-backed
// discovery loads its own cache from Redis during preparation instead.
func (finder *Finder) inheritLocalRegistrations(previous *Finder) {
	if previous.Redis != nil || finder.Redis != nil || previous.runtime == nil || finder.runtime == nil {
		return
	}
	for name, cache := range previous.runtime.confd {
		if next := finder.cacheFor(name); next != nil {
			cache.m.RLock()
			maps.Copy(next.cache, cache.cache)
			cache.m.RUnlock()
		}
	}
}
