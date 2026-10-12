package config

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestDiscoveryConfigurationCanAdvertiseSeparateProtocolPorts(test *testing.T) {
	settings, err := Decode([]byte(`{"finder":{"advice":"127.0.0.1:31935","http_advice":"127.0.0.1:38080"}}`))
	if err != nil {
		test.Fatal(err)
	}
	if err := settings.InitFinder(context.Background()); err != nil {
		test.Fatal(err)
	}
	defer settings.Revoke()
	settings.Finder.Register("live", "sample")
	var record struct {
		Version int    `json:"version"`
		RTMP    string `json:"rtmp"`
		HTTP    string `json:"http"`
	}
	if err := json.Unmarshal([]byte(settings.Finder.GetBackend("live", "sample")), &record); err != nil {
		test.Fatal(err)
	}
	if record.Version != 1 || record.RTMP != "127.0.0.1:31935" || record.HTTP != "127.0.0.1:38080" {
		test.Fatalf("distinct listener endpoints lost: %+v", record)
	}
}

func TestDiscoveryEndpointSelectsProtocolAndPreservesLegacy(test *testing.T) {
	for _, value := range []string{"peer", "peer:31935", "127.0.0.1", "127.0.0.1:38080", "[::1]", "[::1]:31935", "[fe80::1%eth0]:31935"} {
		for _, protocol := range []string{"http", "rtmp"} {
			actual, err := DiscoveryEndpoint(value, protocol)
			if err != nil || actual != value {
				test.Errorf("legacy authority changed: %s %s %q %v", value, protocol, actual, err)
			}
		}
	}
	for _, protocol := range []string{"http", "rtmp"} {
		actual, err := DiscoveryEndpoint("2001:db8::1", protocol)
		if err != nil || actual != "[2001:db8::1]" {
			test.Errorf("bare IPv6 not normalized: %q %v", actual, err)
		}
	}
	encoded, err := discoveryAdvertisement("rtmp.example:31935", "http.example:38080")
	if err != nil {
		test.Fatal(err)
	}
	for protocol, expected := range map[string]string{"rtmp": "rtmp.example:31935", "http": "http.example:38080"} {
		actual, err := DiscoveryEndpoint(encoded, protocol)
		if err != nil || actual != expected {
			test.Errorf("wrong protocol address: %s %q %v", protocol, actual, err)
		}
	}
	if actual, err := discoveryAdvertisement("legacy-peer:31935", ""); err != nil || actual != "legacy-peer:31935" {
		test.Fatal("legacy advertisement was silently rewritten")
	}
	if actual, err := DiscoveryEndpoint("", "http"); err != nil || actual != "" {
		test.Fatal("missing source no longer remains missing")
	}
}

func TestDiscoveryEndpointRejectsInvalidRecordsAndAuthorities(test *testing.T) {
	for _, value := range []string{
		`{"version":2,"rtmp":"peer:1935","http":"peer:8080"}`,
		`{"rtmp":"peer:1935","http":"peer:8080"}`,
		`{"version":1,"rtmp":"peer:1935"}`,
		`{"version":1,"http":"peer:8080"}`,
		`{"version":1,"rtmp":"peer","http":"peer","unknown":true}`,
		`{"version":1,"rtmp":"peer","http":"peer"} {}`,
		`{"version":1,"rtmp":"user:private@peer:1935","http":"peer:8080"}`,
		`{"version":1,"rtmp":"peer:1935","http":"http://peer:8080"}`,
		`{"version":1`, strings.Repeat("x", 4097),
		"http://peer:8080", "user:private@peer", "peer/path", "peer?query", "peer#fragment", "peer\\path",
		"peer:0", "peer:65536", "peer:-1", "peer:+80", "peer:", "peer:port", ":1935", "peer\r\nheader", " peer", "peer ", "[invalid]", "peer%25other",
		"0.0.0.0", "0.0.0.0:1935", "::", "[::]", "[::]:8080",
	} {
		for _, protocol := range []string{"rtmp", "http"} {
			actual, err := DiscoveryEndpoint(value, protocol)
			if err == nil || actual != "" {
				test.Errorf("invalid endpoint accepted: %s %q => %q %v", protocol, value, actual, err)
			}
			if err != nil && strings.Contains(err.Error(), "private") {
				test.Fatal("endpoint diagnostic exposed user information")
			}
		}
	}
	if _, err := DiscoveryEndpoint("peer", "other"); err == nil {
		test.Fatal("unsupported protocol silently used a legacy address")
	}
}

type discoveryWireHook struct {
	value    string
	commands []redis.Cmder
}

func (hook *discoveryWireHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (hook *discoveryWireHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (hook *discoveryWireHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, command redis.Cmder) error {
		hook.commands = append(hook.commands, command)
		switch typed := command.(type) {
		case *redis.StatusCmd:
			hook.value = command.Args()[2].(string)
			typed.SetVal("OK")
		case *redis.StringCmd:
			typed.SetVal(hook.value)
		case *redis.BoolCmd:
			typed.SetVal(true)
		case *redis.IntCmd:
			typed.SetVal(1)
		default:
			return errors.New("unexpected discovery wire command")
		}
		return nil
	}
}

func TestDiscoveryWireRetainsSingleKeyTTLAndOwnershipIdentity(test *testing.T) {
	for _, http := range []string{"", "http.example:38080"} {
		finder := Finder{Advice: "rtmp.example:31935", HTTPAdvice: http, Interval: 1, Expire: 3, Redis: &RedisConfig{Address: "127.0.0.1:1"}}
		if err := finder.init(context.Background(), map[string]*App{"live": {AppName: "live"}}); err != nil {
			test.Fatal(err)
		}
		defer finder.Close()
		finder.runtime.client = redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
		hook := &discoveryWireHook{}
		finder.runtime.client.AddHook(hook)
		var err error
		finder.runtime.advice, err = discoveryAdvertisement(finder.Advice, http)
		if err != nil {
			test.Fatal(err)
		}
		finder.Register("live", "sample")
		actual, err := finder.FindAdvice("live", "sample")
		if err != nil || actual != finder.GetAdvice() || !finder.Keepalive("live", "sample") {
			test.Fatalf("self-ownership comparison changed: %q %v", actual, err)
		}
		finder.Deregister("live", "sample")
		commands := []string{}
		for _, command := range hook.commands {
			commands = append(commands, command.Name())
		}
		if !reflect.DeepEqual(commands, []string{"set", "publish", "get", "expire", "del", "publish"}) {
			test.Fatalf("registration split into extra keys or changed lifetime operations: %v", commands)
		}
		set := hook.commands[0].Args()
		if set[1] != "/streamd/live/sample" || set[2] != finder.GetAdvice() || set[3] != "ex" || set[4] != int64(3) {
			test.Fatalf("wire key/value/TTL changed: %v", set)
		}
	}
}

func TestInvalidDiscoveryEndpointReloadRetainsActiveSnapshot(test *testing.T) {
	filename := filepath.Join(test.TempDir(), "service.json")
	if err := os.WriteFile(filename, []byte(`{"finder":{"advice":"peer:1935","http_advice":"peer:8080"}}`), 0600); err != nil {
		test.Fatal(err)
	}
	manager, err := NewManager(context.Background(), filename)
	if err != nil {
		test.Fatal(err)
	}
	defer func() { Get().Revoke() }()
	GetFinder().Register("live", "sample")
	previous := Get()
	value := previous.Finder.GetBackend("live", "sample")
	if err := os.WriteFile(filename, []byte(`{"finder":{"advice":"peer:1935","http_advice":"http://bad:8080"}}`), 0600); err != nil {
		test.Fatal(err)
	}
	if err := manager.Reload(); err == nil || Get() != previous || manager.Version() != 1 || GetFinder().GetBackend("live", "sample") != value || previous.Finder.ctx.Err() != nil {
		test.Fatal("invalid endpoint changed or revoked the active finder")
	}
}
