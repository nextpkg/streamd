package rtmp

import (
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/rtmp/core"
)

func TestRTMPRejectsUnsafeNamesBeforeSuccess(test *testing.T) {
	connection, listener := lifecycleListener(test)
	config.Get().SetApp("live", &config.App{Enable: true, AppName: "live"})
	for _, name := range []string{"", ".", "..", "../other/sample", "camera/../../other/sample", "./sample", "/sample", "sample/", "camera//front", "camera\\front", "sample\x00", "sample\n", "sample\x7f", "\xff"} {
		for _, method := range []string{core.Publish, core.Play} {
			test.Run(method+"/"+name, func(test *testing.T) {
				transport, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
				if err != nil {
					test.Fatal(err)
				}
				defer transport.Close()
				if err := transport.SetDeadline(time.Now().Add(time.Second)); err != nil {
					test.Fatal(err)
				}
				client := core.NewConnClient("live", name, core.NewConn(transport, core.ConnBufSize))
				err = client.Start(method)
				if err == nil {
					test.Fatal("unsafe stream name received RTMP success")
				}
				if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
					test.Fatalf("unsafe request did not close promptly: %v", err)
				}
			})
		}
	}
	if !test.Failed() && len(connection.StreamKeys()) != 0 {
		test.Fatal("rejected names created stream state")
	}
}
