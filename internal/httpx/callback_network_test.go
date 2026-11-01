package httpx

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCallbackActualConnectionRefusalAndReset(test *testing.T) {
	for _, scenario := range []string{"refusal", "reset"} {
		test.Run(scenario, func(test *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				test.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			address := listener.Addr().String()
			serverDone := make(chan error, 1)
			expected := error(syscall.ECONNREFUSED)
			if scenario == "refusal" {
				if err := listener.Close(); err != nil {
					test.Fatal(err)
				}
			} else {
				expected = syscall.ECONNRESET
				go func() {
					connection, err := listener.Accept()
					if err != nil {
						serverDone <- err
						return
					}
					defer func() { _ = connection.Close() }()
					_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
					request, err := http.ReadRequest(bufio.NewReader(connection))
					if err == nil {
						_, err = io.Copy(io.Discard, request.Body)
						_ = request.Body.Close()
					}
					if err == nil {
						err = connection.(*net.TCPConn).SetLinger(0)
					}
					if err == nil {
						err = connection.Close()
					}
					serverDone <- err
				}()
			}
			executor := newCallbackExecutor()
			defer executor.close()
			err = executor.post("http://private-user:private-password@"+address+"/callback?token=private-token", []byte(`{}`))
			if !errors.Is(err, expected) || strings.Contains(err.Error(), "private-") || strings.Contains(err.Error(), "token=") {
				test.Fatalf("wrong or revealing %s failure: %v", scenario, err)
			}
			if scenario == "reset" {
				select {
				case err := <-serverDone:
					if err != nil {
						test.Fatal(err)
					}
				case <-time.After(time.Second):
					test.Fatal("reset peer did not retire")
				}
			}
		})
	}
}
