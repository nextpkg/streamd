package config

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisDeregisterKeepsBoundedCleanupAfterRuntimeCancellation(test *testing.T) {
	for _, denied := range []bool{false, true} {
		name := "delete-and-notify"
		if denied {
			name = "delete-failure"
		}
		test.Run(name, func(test *testing.T) {
			var cleanupContext context.Context
			commands := []string{}
			finder := preparationFinder(test, func(ctx context.Context, command redis.Cmder) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				deadline, bounded := ctx.Deadline()
				if !bounded || time.Until(deadline) <= 0 || time.Until(deadline) > 2*time.Second {
					test.Error("deregistration cleanup lacks a bounded two-second context")
				}
				if cleanupContext != nil && cleanupContext != ctx {
					test.Error("delete and notification have separate cleanup budgets")
				}
				cleanupContext = ctx
				commands = append(commands, command.Name())
				switch command.Name() {
				case "del":
					if !reflect.DeepEqual(command.Args(), []any{"del", "/streamd/live/sample"}) {
						test.Errorf("wrong deregistration identity: %v", command.Args())
					}
					if denied {
						return errors.New("injected deletion failure")
					}
				case "publish":
					if !reflect.DeepEqual(command.Args(), []any{"publish", "__keyspace@13__:/streamd/live/", "del"}) {
						test.Errorf("changed deletion notification: %v", command.Args())
					}
				default:
					test.Errorf("unexpected cleanup command: %s", command.Name())
				}
				command.(*redis.IntCmd).SetVal(1)
				return nil
			})
			finder.Redis = &RedisConfig{DB: 13}
			finder.runtime.confd["live"] = &confd{prefix: "/streamd/live/", channels: "__keyspace@13__:/streamd/live/"}
			finder.runtime.cancel()
			finder.Deregister("live", "sample")
			expected := []string{"del", "publish"}
			if denied {
				expected = expected[:1]
			}
			if !reflect.DeepEqual(commands, expected) {
				test.Fatalf("runtime cancellation lost cleanup: got %v want %v", commands, expected)
			}
			if cleanupContext == nil || !errors.Is(cleanupContext.Err(), context.Canceled) {
				test.Error("cleanup context was not retired on return")
			}
		})
	}
}
