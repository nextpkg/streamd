package console

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/streamd/config"
)

func enableTestRelay(test *testing.T) {
	test.Helper()
	previous := config.Get().Console.EnableRelay
	config.Get().Console.EnableRelay = true
	test.Cleanup(func() { config.Get().Console.EnableRelay = previous })
}

type observedRelayBody struct {
	io.ReadCloser
	started chan struct{}
	once    sync.Once
}

func (body *observedRelayBody) Read(buffer []byte) (int, error) {
	body.once.Do(func() { close(body.started) })
	return body.ReadCloser.Read(buffer)
}

func TestConsoleCloseRetiresPartialRelayBody(test *testing.T) {
	enableTestRelay(test)
	admin := NewServer(nil)
	router := admin.Init(http.NewServeMux())
	reading, finished := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer close(finished)
		request.Body = &observedRelayBody{ReadCloser: request.Body, started: reading}
		router.ServeHTTP(writer, request)
	}))
	defer server.Close()
	connection, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		test.Fatal(err)
	}
	defer func() { _ = connection.Close(); <-finished; admin.Close() }()
	if _, err := fmt.Fprintf(connection, "POST /admin/relay/start HTTP/1.1\r\nHost: local\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 1000\r\n\r\npull="); err != nil {
		test.Fatal(err)
	}
	select {
	case <-reading:
	case <-time.After(time.Second):
		test.Fatal("request body was not read")
	}
	closed := make(chan struct{})
	go func() { admin.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		test.Error("console close did not finish")
	}
	select {
	case <-finished:
	case <-time.After(300 * time.Millisecond):
		test.Error("partial relay body outlived console close")
	}
}

func TestRelayStartupRespondsToRequestAndConsoleCancellation(test *testing.T) {
	enableTestRelay(test)
	for _, mode := range []string{"request", "console"} {
		test.Run(mode, func(test *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				test.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			accepted := make(chan net.Conn, 1)
			go func() {
				connection, err := listener.Accept()
				if err == nil {
					accepted <- connection
				}
			}()
			admin := NewServer(nil)
			router := admin.Init(http.NewServeMux())
			ctx, cancel := context.WithCancel(context.Background())
			values := url.Values{"pull": {"rtmp://" + listener.Addr().String() + "/live/input"}, "push": {"rtmp://127.0.0.1:1/live/output"}}
			request := httptest.NewRequest(http.MethodPost, "/admin/relay/start", strings.NewReader(values.Encode())).WithContext(ctx)
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			finished := make(chan struct{})
			go func() { defer close(finished); router.ServeHTTP(httptest.NewRecorder(), request) }()
			var peer net.Conn
			select {
			case peer = <-accepted:
			case <-time.After(time.Second):
				cancel()
				test.Fatal("relay did not dial")
			}
			defer func() { _ = peer.Close(); cancel(); <-finished; admin.Close() }()
			if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				test.Fatal(err)
			}
			if _, err := io.ReadFull(peer, make([]byte, 1537)); err != nil {
				test.Fatal(err)
			}
			closed := make(chan struct{})
			if mode == "request" {
				cancel()
				close(closed)
			} else {
				go func() { admin.Close(); close(closed) }()
			}
			select {
			case <-finished:
			case <-time.After(300 * time.Millisecond):
				test.Error("cancelled relay startup remained blocked in handshake")
			}
			select {
			case <-closed:
			case <-time.After(300 * time.Millisecond):
				test.Error("console close waited for relay dial timeout")
			}
		})
	}
}

func TestRelayDNSUsesRequestAndOwnerCancellation(test *testing.T) {
	enableTestRelay(test)
	for _, mode := range []string{"request", "console", "deadline"} {
		test.Run(mode, func(test *testing.T) {
			sink, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				test.Fatal(err)
			}
			queried, drained := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(drained)
				var once sync.Once
				buffer := make([]byte, 4096)
				for {
					if _, _, err := sink.ReadFrom(buffer); err != nil {
						return
					}
					once.Do(func() { close(queried) })
				}
			}()
			defer func() { _ = sink.Close(); <-drained }()
			resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "udp", sink.LocalAddr().String())
			}}
			admin := NewServer(nil)
			budget := make(chan time.Duration, 1)
			admin.resolveIP = func(ctx context.Context, host string) ([]net.IPAddr, error) {
				deadline, ok := ctx.Deadline()
				if !ok {
					budget <- -1
				} else {
					budget <- time.Until(deadline)
				}
				return resolver.LookupIPAddr(ctx, host)
			}
			router := admin.Init(http.NewServeMux())
			ctx, cancel := context.WithCancel(context.Background())
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
			}
			defer cancel()
			values := url.Values{"pull": {"rtmp://" + mode + ".streamd.invalid./live/input"}, "push": {"rtmp://127.0.0.1/live/output"}}
			request := httptest.NewRequest(http.MethodPost, "/admin/relay/start", strings.NewReader(values.Encode())).WithContext(ctx)
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			finished := make(chan struct{})
			go func() { defer close(finished); router.ServeHTTP(httptest.NewRecorder(), request) }()
			defer func() { cancel(); <-finished; admin.Close() }()
			select {
			case <-queried:
			case <-time.After(time.Second):
				test.Fatal("local DNS sink did not receive a query")
			}
			if remaining := <-budget; remaining <= 0 || remaining > queryIOBudget {
				test.Fatalf("DNS lacks management deadline: %v", remaining)
			}
			closed := make(chan struct{})
			switch mode {
			case "request":
				cancel()
				close(closed)
			case "console":
				go func() { admin.Close(); close(closed) }()
			default:
				close(closed)
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				test.Error("DNS outlived cancellation")
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				test.Error("close waited for DNS retry timeout")
			}
		})
	}
}

func TestRelayGateWaitIsCancellable(test *testing.T) {
	admin := NewServer(nil)
	if !admin.lockRelay(context.Background()) {
		test.Fatal("cannot acquire test gate")
	}
	defer admin.Close()
	defer admin.unlockRelay()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan bool, 1)
	go func() { finished <- admin.lockRelay(ctx) }()
	cancel()
	select {
	case acquired := <-finished:
		if acquired {
			test.Fatal("cancelled request acquired held gate")
		}
	case <-time.After(time.Second):
		test.Fatal("cancelled operation waited for held gate")
	}
}

func TestClosedConsoleRejectsPartialRelayPostWithoutDraining(test *testing.T) {
	enableTestRelay(test)
	admin := NewServer(nil)
	admin.Close()
	server := httptest.NewServer(admin.Init(http.NewServeMux()))
	defer server.Close()
	connection, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		test.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
		test.Fatal(err)
	}
	if _, err := fmt.Fprintf(connection, "POST /admin/relay/start HTTP/1.1\r\nHost: local\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 1000\r\n\r\npull="); err != nil {
		test.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		test.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusServiceUnavailable {
		test.Fatalf("status=%d", response.StatusCode)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		test.Fatal(err)
	}
}
