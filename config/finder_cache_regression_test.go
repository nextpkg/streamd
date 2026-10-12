package config

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type discoveryReadHook struct {
	failure  error
	commands *[]redis.Cmder
	keys     []string
}

func (hook discoveryReadHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (hook discoveryReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (hook discoveryReadHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, command redis.Cmder) error {
		if hook.commands != nil {
			*hook.commands = append(*hook.commands, command)
		}
		switch typed := command.(type) {
		case *redis.StatusCmd:
			typed.SetVal("none")
		case *redis.ScanCmd:
			keys := hook.keys
			if keys == nil {
				keys = []string{"/streamd/live/known"}
			}
			typed.SetVal(keys, 0)
		case *redis.StringCmd:
			typed.SetVal("fresh-peer")
			typed.SetErr(hook.failure)
			return hook.failure
		default:
			return errors.New("unexpected discovery command")
		}
		return nil
	}
}

func TestFinderCachedBackendSupportsCustomSeparator(test *testing.T) {
	for _, separator := range []string{"/", "#"} {
		finder := Finder{Interval: 1, Expire: 3, Separator: separator}
		require.NoError(test, finder.init(context.Background(), map[string]*App{"live": {AppName: "live"}}))
		defer finder.Close()
		require.Equal(test, "/streamd/live/nested/channel", finder.clean(finder.confd["live"].prefix+"nested/channel"))
		finder.confd["live"].cache[finder.clean(finder.confd["live"].prefix+"nested/channel")] = "peer"
		require.Equal(test, "peer", finder.GetBackend("live", "nested/channel"), separator)
		finder.Register("live", "local")
		require.Equal(test, "127.0.0.1", finder.GetBackend("live", "local"))
		finder.Deregister("live", "local")
		require.Empty(test, finder.GetBackend("live", "local"))
	}
}

func TestFinderCacheRetainsSnapshotOnReadFailure(test *testing.T) {
	for _, example := range []struct {
		name    string
		failure error
		want    string
	}{
		{name: "read failed", failure: io.ErrUnexpectedEOF, want: "confirmed-peer"},
		{name: "key expired between scan and get", failure: redis.Nil},
		{name: "read succeeded", want: "fresh-peer"},
	} {
		test.Run(example.name, func(test *testing.T) {
			finder := Finder{Interval: 1, Expire: 3, Separator: "/"}
			require.NoError(test, finder.init(context.Background(), map[string]*App{"live": {AppName: "live"}}))
			defer finder.Close()
			finder.runtime.client = redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
			finder.runtime.client.AddHook(discoveryReadHook{failure: example.failure})
			finder.confd["live"].cache["/streamd/live/known"] = "confirmed-peer"
			err := finder.store(finder.confd["live"])
			if example.failure != nil && example.failure != redis.Nil {
				require.ErrorIs(test, err, example.failure)
			} else {
				require.NoError(test, err)
			}
			require.Equal(test, example.want, finder.GetBackend("live", "known"))
		})
	}
}

func TestFinderRedisScanUsesCustomSeparatorWithoutDuplicatePrefix(test *testing.T) {
	finder := Finder{Interval: 1, Expire: 3, Separator: "#"}
	require.NoError(test, finder.init(context.Background(), map[string]*App{"live": {AppName: "live"}}))
	defer finder.Close()
	var commands []redis.Cmder
	finder.runtime.client = redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	finder.runtime.client.AddHook(discoveryReadHook{commands: &commands, keys: []string{"#streamd#live#known"}})
	require.NoError(test, finder.store(finder.confd["live"]))
	require.Equal(test, "#streamd#live", commands[0].Args()[1])
	require.Equal(test, "#streamd#live#*", commands[1].Args()[3])
	require.Equal(test, "fresh-peer", finder.GetBackend("live", "known"))
}
