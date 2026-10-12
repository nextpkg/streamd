package rtmp

import (
	"context"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/stretchr/testify/assert"
)

func TestFind(t *testing.T) {
	at := assert.New(t)

	ds := &sourceCache{
		data: nil,
		time: 0,
	}

	app := &config.App{
		Enable:     true,
		AppName:    "t_app",
		StaticPush: nil,
		StaticPull: nil,
	}

	config.Get().SetApp("t_app", app)

	conn := NewConn(context.Background())
	info := base.NewInfo("t_app", "t_instance", false)
	conn.find(ds, info)
	at.Equal(0, len(ds.data))

	app.StaticPull = append(app.StaticPull, "rtmp://127.0.0.1/source")
	conn.find(ds, info)
	at.Equal(1, len(ds.data))
}
