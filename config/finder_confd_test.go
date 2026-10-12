package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinder_confd(t *testing.T) {
	at := require.New(t)

	sep := cfgFinder.Separator

	cfgFinder.Separator = "#"
	at.Equal("#test#1", cfgFinder.transform("/test/1"))
	at.Equal("#test##1", cfgFinder.transform("/test//1"))
	cfgFinder.Separator = "/"
	at.Equal("/test", cfgFinder.transform("/test"))

	cfgFinder.Separator = "#"
	at.Equal("/test/1/2", cfgFinder.clean("test/1/2"))

	cfgFinder.Separator = "/"
	at.Equal("/test/1/2", cfgFinder.clean("/test/1/2"))

	cfgFinder.Separator = sep
}
