package rtmp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/rtmp/core"
)

func TestApplicationReloadRejectsLatePublishWithoutReplacingLive(test *testing.T) {
	lifecycleConfiguration(test)
	for _, state := range []string{"disabled", "removed"} {
		for _, warm := range []bool{false, true} {
			name := state + "/cold"
			if warm {
				name = state + "/live"
			}
			test.Run(name, func(test *testing.T) {
				settings := config.NewConfig()
				settings.App = []config.App{{Enable: true, AppName: "admission"}}
				filename := filepath.Join(test.TempDir(), "config.json")
				write := func() {
					test.Helper()
					encoded, err := json.Marshal(settings)
					if err != nil {
						test.Fatal(err)
					}
					if err := os.WriteFile(filename, encoded, 0600); err != nil {
						test.Fatal(err)
					}
				}
				write()
				manager, err := config.NewManager(context.Background(), filename)
				if err != nil {
					test.Fatal(err)
				}
				reload := func() {
					test.Helper()
					write()
					if err := manager.Reload(); err != nil {
						test.Fatal(err)
					}
				}
				connection := NewConn(context.Background())
				defer connection.Close()
				makeReader := func() *takeoverReader {
					reader := &takeoverReader{Universal: core.NewUniversal(base.NewInfo("admission", "sample", false)), done: make(chan struct{})}
					test.Cleanup(reader.Close)
					return reader
				}
				previous := makeReader()
				writer := core.NewFunnel(&takeoverTerminal{Universal: core.NewUniversal(base.NewInfo("admission", "sample", true))})
				defer func() { writer.Close(); writer.Wait() }()
				if warm {
					connection.InstallReader(previous)
					connection.InstallWriter(writer)
				}
				late := makeReader()
				if state == "disabled" {
					settings.App[0].Enable = false
				} else {
					settings.App = nil
				}
				reload()
				func() {
					defer func() {
						if failure := recover(); failure != nil {
							test.Fatalf("late publisher panicked after application %s: %v", state, failure)
						}
					}()
					connection.InstallReader(late)
				}()
				select {
				case <-late.done:
				default:
					test.Fatal("late publisher retained after application removal")
				}
				if warm {
					current, exists := connection.stream.Load(previous.Info().Key)
					if !exists || current.(*Stream).Reader().ReadCloser != previous || writer.IsClosed() {
						test.Fatal("rejected publisher displaced the live publication or its viewer")
					}
					select {
					case <-previous.done:
						test.Fatal("application removal closed the existing publisher")
					default:
					}
				} else if len(connection.StreamKeys()) != 0 {
					test.Fatal("rejected publisher left a placeholder stream")
				}
				settings.App = []config.App{{Enable: true, AppName: "admission"}}
				reload()
				replacement := makeReader()
				connection.InstallReader(replacement)
				current, exists := connection.stream.Load(replacement.Info().Key)
				if !exists || current.(*Stream).Reader().ReadCloser != replacement {
					test.Fatal("application reenable did not admit a new publisher")
				}
				if warm {
					select {
					case <-previous.done:
					default:
						test.Fatal("accepted takeover left old publisher alive")
					}
					if count, _ := connection.WriterActive(replacement.Info().Key); count != 1 || writer.IsClosed() {
						test.Fatal("reenabled takeover lost its existing viewer")
					}
				}
			})
		}
	}
}

func TestPasserbyRetainsIdlePolicyAndPublicationSnapshot(test *testing.T) {
	lifecycleConfiguration(test)
	for _, passerby := range []bool{false, true} {
		for _, external := range []bool{false, true} {
			settings := config.NewConfig()
			application := &config.App{Enable: true, AppName: "passerby", Passerby: passerby}
			stream := newStream(base.NewInfo("passerby", "sample", external), settings, application)
			reader := &viewerActivityReader{takeoverReader: &takeoverReader{Universal: core.NewUniversal(stream.info), done: make(chan struct{})}}
			stream.rConn = &StreamReader{ReadCloser: reader}
			config.Get().SetApp("passerby", &config.App{Enable: true, AppName: "passerby", Passerby: !passerby})
			stream.active.time = time.Now().Add(-time.Hour).Unix()
			if alive := stream.health(); alive != (!external && !passerby) || stream.app != application {
				test.Fatalf("idle policy changed: external=%t passerby=%t alive=%t", external, passerby, alive)
			}
			stream.active.count.Store(1)
			if !stream.health() {
				test.Fatal("active viewer did not retain publication")
			}
			stream.active.count.Store(0)
			if !stream.health() {
				test.Fatal("last viewer leaving bypassed the idle grace period")
			}
			reader.expired = true
			if stream.health() {
				test.Fatal("Passerby or viewer activity retained a timed-out publisher")
			}
		}
	}
}

func TestUnavailableApplicationsCloseRTMPRequestsWithoutStreams(test *testing.T) {
	connection, listener := lifecycleListener(test)
	for _, app := range []string{"missing", "", "live/nested"} {
		for _, method := range []string{core.Play, core.Publish} {
			test.Run(app+"/"+method, func(test *testing.T) {
				transport, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
				if err != nil {
					test.Fatal(err)
				}
				defer transport.Close()
				if err := transport.SetDeadline(time.Now().Add(time.Second)); err != nil {
					test.Fatal(err)
				}
				client := core.NewConnClient(app, "sample", core.NewConn(transport, core.ConnBufSize))
				err = client.Start(method)
				for err == nil {
					var response core.ChunkStream
					err = client.Read(&response)
				}
				if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
					test.Fatalf("unavailable application did not close its RTMP connection: %v", err)
				}
				if len(connection.StreamKeys()) != 0 {
					test.Fatal("unavailable application created a stream")
				}
			})
		}
	}
}
