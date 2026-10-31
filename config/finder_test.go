package config

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var cfgFinder = &Finder{
	Advice:   "127.0.0.1",
	Expire:   60,
	Interval: 30,
}

func TestFinder_init(t *testing.T) {
	at := require.New(t)

	at.Nil(cfgFinder.init(context.Background(), nil))
	at.Equal(time.Duration(60), cfgFinder.Expire)
	at.Equal(time.Duration(30), cfgFinder.Interval)
	at.Len(cfgFinder.runtime.confd, 0)

	apps := make(map[string]*App)
	apps["test1"] = nil
	apps["test2"] = nil
	at.Nil(cfgFinder.init(context.Background(), apps))

	at.Len(cfgFinder.runtime.confd, 2)
	at.Equal("/streamd/test1/", cfgFinder.runtime.confd["test1"].prefix)
	at.Equal("__keyspace@0__:/streamd/test1/", cfgFinder.runtime.confd["test1"].channels)
	at.Len(cfgFinder.runtime.confd["test1"].cache, 0)

	at.Equal("127.0.0.1", cfgFinder.Advice)
	at.Equal("", cfgFinder.GetAdvice())
}
