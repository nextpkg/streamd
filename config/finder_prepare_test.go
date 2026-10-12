package config

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type prepareRedisHook struct {
	process func(context.Context, redis.Cmder) error
}

func (hook prepareRedisHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (hook prepareRedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (hook prepareRedisHook) ProcessHook(redis.ProcessHook) redis.ProcessHook { return hook.process }

func preparationFinder(test *testing.T, process func(context.Context, redis.Cmder) error) *Finder {
	test.Helper()
	finder := &Finder{Interval: 1, Expire: 5, Separator: "/"}
	if err := finder.init(context.Background(), nil); err != nil {
		test.Fatal(err)
	}
	finder.runtime.client = redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	finder.runtime.client.AddHook(prepareRedisHook{process: process})
	test.Cleanup(finder.Close)
	return finder
}

func TestRedisPreparationChecksWritesAndDeletesOnlyOwnedProbe(test *testing.T) {
	for _, denied := range []string{"", "set", "get", "expire", "del", "publish", "collision", "changed", "disappeared"} {
		test.Run(denied, func(test *testing.T) {
			var key string
			deleted := 0
			finder := preparationFinder(test, func(ctx context.Context, command redis.Cmder) error {
				name := command.Name()
				args := command.Args()
				if name == "set" {
					key = args[1].(string)
					if !strings.HasPrefix(key, "/streamd/live/__streamd_probe_") || len(args) != 6 || args[2] != "" || args[3] != "ex" || args[4] != int64(5) || args[5] != "nx" {
						test.Errorf("unsafe probe SET: %v", args)
					}
				}
				if name == "del" {
					deleted++
				}
				if name != "publish" && args[1] != key {
					test.Error("probe changed a different key")
				}
				if name == denied {
					return errors.New("NOPERM denied " + name)
				}
				switch typed := command.(type) {
				case *redis.BoolCmd:
					typed.SetVal(denied != "collision" && denied != "disappeared")
					if name == "set" && denied == "disappeared" {
						typed.SetVal(true)
					}
				case *redis.StringCmd:
					if denied == "changed" {
						typed.SetVal("other")
					} else {
						typed.SetVal("")
					}
				case *redis.IntCmd:
					typed.SetVal(1)
				default:
					test.Errorf("unexpected probe command: %T", command)
				}
				return nil
			})
			err := finder.prepareWrites(context.Background(), &confd{prefix: "/streamd/live/", channels: "__keyspace@13__:/streamd/live/"})
			if (err != nil) != (denied != "") {
				test.Fatalf("preparation result for %q: %v", denied, err)
			}
			expectedDeletes := 1
			if denied == "set" || denied == "collision" {
				expectedDeletes = 0
			}
			if deleted != expectedDeletes {
				test.Fatalf("cleanup calls %d, want %d", deleted, expectedDeletes)
			}
		})
	}
}

func TestRedisPreparationCancellationStillCleansOwnedProbe(test *testing.T) {
	cleaned := false
	finder := preparationFinder(test, func(ctx context.Context, command redis.Cmder) error {
		switch command.Name() {
		case "set":
			command.(*redis.BoolCmd).SetVal(true)
		case "get":
			<-ctx.Done()
			return ctx.Err()
		case "del":
			deadline, bounded := ctx.Deadline()
			cleaned = ctx.Err() == nil && bounded && time.Until(deadline) <= time.Second
			command.(*redis.IntCmd).SetVal(1)
		default:
			test.Errorf("unexpected canceled preparation command %s", command.Name())
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := finder.prepareWrites(ctx, &confd{prefix: "/streamd/live/"})
	if !errors.Is(err, context.DeadlineExceeded) || !cleaned || time.Since(started) > time.Second {
		test.Fatalf("canceled probe cleanup failed: cleaned=%v err=%v", cleaned, err)
	}
}

func TestRedisCandidateCacheUsesPreparationBudgetWithoutErasingSnapshot(test *testing.T) {
	finder := preparationFinder(test, func(ctx context.Context, command redis.Cmder) error {
		if command.Name() == "type" {
			command.(*redis.StatusCmd).SetVal("none")
			return nil
		}
		<-ctx.Done()
		return ctx.Err()
	})
	cache := &confd{prefix: "/streamd/live/", cache: map[string]string{"/streamd/live/sample": "confirmed"}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := finder.storeContext(ctx, cache); !errors.Is(err, context.DeadlineExceeded) {
		test.Fatalf("preparation budget not enforced: %v", err)
	}
	if finder.ctx.Err() != nil || cache.getValue("/streamd/live/sample") != "confirmed" {
		test.Fatal("preparation canceled runtime or replaced confirmed cache")
	}
}
