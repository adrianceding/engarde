package clientrole

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	"github.com/adrianceding/engarde/internal/serverrole"
	"github.com/adrianceding/engarde/internal/socks5"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

var reservedTCPTestAddresses sync.Map

func TestTCPSOCKS5EndToEndWithRedundantCarriers(t *testing.T) {
	echoListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	destinationAccepted := make(chan struct{}, 2)
	go func() {
		for {
			conn, acceptErr := echoListener.Accept()
			if acceptErr != nil {
				return
			}
			destinationAccepted <- struct{}{}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	serverAddress := freeTCPAddress(t)
	clientAddress := freeTCPAddress(t)
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := serverrole.New(config.Server{ListenAddr: serverAddress, AllowUnsafeDynamicDestination: true, Transfer: transfer}, "test", nil)
	client := New(config.Client{ListenAddr: clientAddress, DstAddr: serverAddress, Transfer: transfer}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}, {Index: 2, Name: "path-b"}}, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }
	previousDial := dialTCPOnInterface
	dialTCPOnInterface = func(ctx context.Context, destination, _, _ string, timeout time.Duration) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, "tcp4", destination)
	}
	t.Cleanup(func() { dialTCPOnInterface = previousDial })

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	clientDone := make(chan error, 1)
	go func() { serverDone <- server.Run(ctx) }()
	waitForTCPListener(t, serverAddress)
	go func() { clientDone <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-clientDone:
			if err != nil {
				t.Errorf("client Run error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("client did not stop")
		}
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("server Run error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
	})

	waitForTCPStatus(t, func() (int, int, int) {
		clientStatusValue, clientErr := client.Status()
		if clientErr != nil {
			return -1, -1, -1
		}
		serverStatusValue, serverErr := server.Status()
		if serverErr != nil {
			return -1, -1, -1
		}
		clientStatus := clientStatusValue.(control.ClientStatus)
		serverStatus := serverStatusValue.(control.ServerStatus)
		return clientStatus.Sessions, serverStatus.Sessions, serverStatus.Streams
	}, 2, 2, 0)
	select {
	case <-destinationAccepted:
		t.Fatal("destination was dialed before a logical stream was opened")
	case <-time.After(50 * time.Millisecond):
	}

	application := dialSOCKS5Eventually(t, clientAddress, echoListener.Addr().String())
	defer application.Close()
	want := []byte("redundant tcp socks5")
	if _, err := application.Write(want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(application, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("echo = %q, want %q", got, want)
	}
	waitForTCPTraffic(t, client, server)
	waitForTCPStatus(t, func() (int, int, int) {
		clientStatusValue, _ := client.Status()
		serverStatusValue, _ := server.Status()
		clientStatus := clientStatusValue.(control.ClientStatus)
		serverStatus := serverStatusValue.(control.ServerStatus)
		return clientStatus.Carriers, clientStatus.Sessions, serverStatus.Sessions
	}, 2, 2, 2)
	select {
	case <-destinationAccepted:
	case <-time.After(time.Second):
		t.Fatal("destination was not dialed")
	}
	select {
	case <-destinationAccepted:
		t.Fatal("destination was dialed more than once for one logical stream")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestTCPPathSessionsContinueConnectingAfterFlowAssignment(t *testing.T) {
	echoListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	go func() {
		for {
			conn, acceptErr := echoListener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	serverAddress := freeTCPAddress(t)
	clientAddress := freeTCPAddress(t)
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := serverrole.New(config.Server{ListenAddr: serverAddress, AllowUnsafeDynamicDestination: true, Transfer: transfer}, "test", nil)
	client := New(config.Client{ListenAddr: clientAddress, DstAddr: serverAddress, Transfer: transfer}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}, {Index: 2, Name: "path-b"}}, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }
	allowPathB := make(chan struct{})
	previousDial := dialTCPOnInterface
	dialTCPOnInterface = func(ctx context.Context, destination, _, interfaceName string, timeout time.Duration) (net.Conn, error) {
		if interfaceName == "path-b" {
			select {
			case <-allowPathB:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, "tcp4", destination)
	}
	t.Cleanup(func() { dialTCPOnInterface = previousDial })

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	clientDone := make(chan error, 1)
	go func() { serverDone <- server.Run(ctx) }()
	waitForTCPListener(t, serverAddress)
	go func() { clientDone <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		waitForTCPShutdown(t, "client", clientDone)
		waitForTCPShutdown(t, "server", serverDone)
	})

	waitForTCPStatus(t, func() (int, int, int) {
		clientStatusValue, clientErr := client.Status()
		serverStatusValue, serverErr := server.Status()
		if clientErr != nil || serverErr != nil {
			return -1, -1, -1
		}
		clientStatus := clientStatusValue.(control.ClientStatus)
		serverStatus := serverStatusValue.(control.ServerStatus)
		return clientStatus.Sessions, serverStatus.Sessions, clientStatus.Carriers
	}, 1, 1, 0)

	application := dialSOCKS5Eventually(t, clientAddress, echoListener.Addr().String())
	defer application.Close()
	want := []byte("partial path availability")
	if _, err := application.Write(want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(application, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("echo = %q, want %q", got, want)
	}
	waitForTCPStatus(t, func() (int, int, int) {
		statusValue, statusErr := client.Status()
		if statusErr != nil {
			return -1, -1, -1
		}
		status := statusValue.(control.ClientStatus)
		return status.Streams, status.Carriers, status.Sessions
	}, 1, 1, 1)

	close(allowPathB)
	waitForTCPStatus(t, func() (int, int, int) {
		statusValue, statusErr := client.Status()
		if statusErr != nil {
			return -1, -1, -1
		}
		status := statusValue.(control.ClientStatus)
		return status.Streams, status.Carriers, status.Sessions
	}, 1, 2, 2)
}

func TestTCPAssignedGroupReconnectsCarrier(t *testing.T) {
	echoListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	go func() {
		for {
			conn, acceptErr := echoListener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	serverAddress := freeTCPAddress(t)
	clientAddress := freeTCPAddress(t)
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := serverrole.New(config.Server{ListenAddr: serverAddress, AllowUnsafeDynamicDestination: true, Transfer: transfer}, "test", nil)
	client := New(config.Client{ListenAddr: clientAddress, DstAddr: serverAddress, Transfer: transfer}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}, {Index: 2, Name: "path-b"}}, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }
	previousDial := dialTCPOnInterface
	dialTCPOnInterface = func(ctx context.Context, destination, _, _ string, timeout time.Duration) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, "tcp4", destination)
	}
	t.Cleanup(func() { dialTCPOnInterface = previousDial })

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	clientDone := make(chan error, 1)
	go func() { serverDone <- server.Run(ctx) }()
	waitForTCPListener(t, serverAddress)
	go func() { clientDone <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		waitForTCPShutdown(t, "client", clientDone)
		waitForTCPShutdown(t, "server", serverDone)
	})

	application := dialSOCKS5Eventually(t, clientAddress, echoListener.Addr().String())
	defer application.Close()
	waitForTCPStatus(t, func() (int, int, int) {
		statusValue, statusErr := client.Status()
		if statusErr != nil {
			return -1, -1, -1
		}
		status := statusValue.(control.ClientStatus)
		return status.Streams, status.Carriers, status.Sessions
	}, 1, 2, 2)

	runtime := client.getTCPRuntime()
	runtime.mu.Lock()
	var streamID tcpstream.StreamID
	for id := range runtime.flows {
		streamID = id
	}
	carrier := runtime.carriers[streamID]["path-b"]
	runtime.mu.Unlock()
	if carrier == nil {
		t.Fatal("path-b carrier was not registered")
	}
	carrier.Close()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runtime.mu.Lock()
		replacement := runtime.carriers[streamID]["path-b"]
		runtime.mu.Unlock()
		if replacement != nil && replacement != carrier {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	runtime.mu.Lock()
	replacement := runtime.carriers[streamID]["path-b"]
	runtime.mu.Unlock()
	if replacement == nil || replacement == carrier {
		t.Fatal("path-b carrier was not replaced by its group")
	}
	waitForTCPStatus(t, func() (int, int, int) {
		statusValue, statusErr := client.Status()
		if statusErr != nil {
			return -1, -1, -1
		}
		status := statusValue.(control.ClientStatus)
		return status.Streams, status.Carriers, status.Sessions
	}, 1, 2, 2)

	want := []byte("group reconnect")
	if _, err := application.Write(want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(application, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("echo = %q, want %q", got, want)
	}
}

func TestTCPGroupsTrackInterfaceTopology(t *testing.T) {
	echoListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	go func() {
		for {
			conn, acceptErr := echoListener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	serverAddress := freeTCPAddress(t)
	clientAddress := freeTCPAddress(t)
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := serverrole.New(config.Server{ListenAddr: serverAddress, AllowUnsafeDynamicDestination: true, Transfer: transfer}, "test", nil)
	client := New(config.Client{ListenAddr: clientAddress, DstAddr: serverAddress, Transfer: transfer}, "test", nil)
	var includePathB atomic.Bool
	client.listInterfaces = func() ([]net.Interface, error) {
		interfaces := []net.Interface{{Index: 1, Name: "path-a"}}
		if includePathB.Load() {
			interfaces = append(interfaces, net.Interface{Index: 2, Name: "path-b"})
		}
		return interfaces, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }
	previousDial := dialTCPOnInterface
	dialTCPOnInterface = func(ctx context.Context, destination, _, _ string, timeout time.Duration) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, "tcp4", destination)
	}
	t.Cleanup(func() { dialTCPOnInterface = previousDial })

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	clientDone := make(chan error, 1)
	go func() { serverDone <- server.Run(ctx) }()
	waitForTCPListener(t, serverAddress)
	go func() { clientDone <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		waitForTCPShutdown(t, "client", clientDone)
		waitForTCPShutdown(t, "server", serverDone)
	})

	application := dialSOCKS5Eventually(t, clientAddress, echoListener.Addr().String())
	defer application.Close()
	waitForTCPStatus(t, func() (int, int, int) {
		statusValue, statusErr := client.Status()
		if statusErr != nil {
			return -1, -1, -1
		}
		status := statusValue.(control.ClientStatus)
		return status.Streams, status.Carriers, status.Sessions
	}, 1, 1, 1)

	includePathB.Store(true)
	client.getTCPRuntime().refresh()
	waitForTCPStatus(t, func() (int, int, int) {
		statusValue, statusErr := client.Status()
		if statusErr != nil {
			return -1, -1, -1
		}
		status := statusValue.(control.ClientStatus)
		return status.Streams, status.Carriers, status.Sessions
	}, 1, 2, 2)

	includePathB.Store(false)
	client.getTCPRuntime().refresh()
	waitForTCPStatus(t, func() (int, int, int) {
		statusValue, statusErr := client.Status()
		if statusErr != nil {
			return -1, -1, -1
		}
		status := statusValue.(control.ClientStatus)
		return status.Streams, status.Carriers, status.Sessions
	}, 1, 1, 1)

	want := []byte("topology update")
	if _, err := application.Write(want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(application, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("echo = %q, want %q", got, want)
	}
}

func TestTCPSOCKS5DynamicDestinations(t *testing.T) {
	firstListener := startTaggedTCPServer(t, "first:")
	secondListener := startTaggedTCPServer(t, "second:")

	serverAddress := freeTCPAddress(t)
	clientAddress := freeTCPAddress(t)
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := serverrole.New(config.Server{
		ListenAddr:                    serverAddress,
		AllowUnsafeDynamicDestination: true,
		Transfer:                      transfer,
	}, "test", nil)
	client := New(config.Client{
		ListenAddr: clientAddress,
		DstAddr:    serverAddress,
		Transfer:   transfer,
	}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}, {Index: 2, Name: "path-b"}}, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }
	previousDial := dialTCPOnInterface
	dialTCPOnInterface = func(ctx context.Context, destination, _, _ string, timeout time.Duration) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, "tcp4", destination)
	}
	t.Cleanup(func() { dialTCPOnInterface = previousDial })

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	clientDone := make(chan error, 1)
	go func() { serverDone <- server.Run(ctx) }()
	waitForTCPListener(t, serverAddress)
	go func() { clientDone <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		waitForTCPShutdown(t, "client", clientDone)
		waitForTCPShutdown(t, "server", serverDone)
	})

	waitForTCPStatus(t, func() (int, int, int) {
		clientStatusValue, clientErr := client.Status()
		serverStatusValue, serverErr := server.Status()
		if clientErr != nil || serverErr != nil {
			return -1, -1, -1
		}
		clientStatus := clientStatusValue.(control.ClientStatus)
		serverStatus := serverStatusValue.(control.ServerStatus)
		return clientStatus.Sessions, serverStatus.Sessions, serverStatus.Streams
	}, 2, 2, 0)

	_, firstPort, err := net.SplitHostPort(firstListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	assertSOCKS5TaggedEcho(t, clientAddress, net.JoinHostPort("localhost", firstPort), "first:")
	assertSOCKS5TaggedEcho(t, clientAddress, secondListener.Addr().String(), "second:")
	t.Run("IPv6 destination", func(t *testing.T) {
		listener, err := net.Listen("tcp6", "[::1]:0")
		if err != nil {
			t.Skipf("IPv6 loopback is unavailable: %v", err)
		}
		serveTaggedTCPServer(t, listener, "ipv6:")
		assertSOCKS5TaggedEcho(t, clientAddress, listener.Addr().String(), "ipv6:")
	})
}

func TestTCPSOCKS5ReportsConnectionRefused(t *testing.T) {
	serverAddress := freeTCPAddress(t)
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.DialTimeoutMillis = 100
	transfer.TCP.OpenTimeoutMillis = 300
	server := serverrole.New(config.Server{
		ListenAddr:                    serverAddress,
		AllowUnsafeDynamicDestination: true,
		Transfer:                      transfer,
	}, "test", nil)
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		waitForTCPShutdown(t, "server", serverDone)
	})
	waitForTCPListener(t, serverAddress)

	clientAddress := freeTCPAddress(t)
	client := New(config.Client{
		ListenAddr: clientAddress,
		DstAddr:    serverAddress,
		Transfer:   transfer,
	}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}}, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }
	previousDial := dialTCPOnInterface
	dialTCPOnInterface = func(ctx context.Context, destination, _, _ string, timeout time.Duration) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, "tcp4", destination)
	}
	t.Cleanup(func() { dialTCPOnInterface = previousDial })

	clientDone := make(chan error, 1)
	go func() { clientDone <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		waitForTCPShutdown(t, "client", clientDone)
	})
	waitForTCPStatus(t, func() (int, int, int) {
		clientStatusValue, clientErr := client.Status()
		serverStatusValue, serverErr := server.Status()
		if clientErr != nil || serverErr != nil {
			return -1, -1, -1
		}
		clientStatus := clientStatusValue.(control.ClientStatus)
		serverStatus := serverStatusValue.(control.ServerStatus)
		return clientStatus.Sessions, serverStatus.Sessions, serverStatus.Streams
	}, 1, 1, 0)

	// Allocate the refused destination after both Engarde listeners are bound so
	// neither listener can accidentally reuse this port.
	closedDestination, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	destinationAddress := closedDestination.Addr().String()
	if err := closedDestination.Close(); err != nil {
		t.Fatal(err)
	}

	conn := dialTCPEventually(t, clientAddress)
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	negotiateSOCKS5(t, conn, "", "")
	writeSOCKS5Connect(t, conn, destinationAddress)
	if reply := readSOCKS5Reply(t, conn); reply != byte(socks5.ReplyConnectionRefused) {
		t.Fatalf("SOCKS5 CONNECT reply = %d, want connection refused", reply)
	}
}

func TestTCPSOCKS5AndPeerAuthentication(t *testing.T) {
	destinationListener := startTaggedTCPServer(t, "authenticated:")
	serverAddress := freeTCPAddress(t)
	clientAddress := freeTCPAddress(t)
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := serverrole.New(config.Server{
		ListenAddr: serverAddress,
		PeerAuth:   &config.ServerPeerAuth{Users: map[string]string{"client-a": "peer-secret"}},
		Transfer:   transfer,
	}, "test", nil)
	client := New(config.Client{
		ListenAddr: clientAddress,
		DstAddr:    serverAddress,
		SOCKS5Auth: &config.Credentials{Username: "client", Password: "local-secret"},
		PeerAuth:   &config.Credentials{Username: "client-a", Password: "peer-secret"},
		Transfer:   transfer,
	}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}}, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }
	previousDial := dialTCPOnInterface
	dialTCPOnInterface = func(ctx context.Context, destination, _, _ string, timeout time.Duration) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, "tcp4", destination)
	}
	t.Cleanup(func() { dialTCPOnInterface = previousDial })

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	clientDone := make(chan error, 1)
	go func() { serverDone <- server.Run(ctx) }()
	waitForTCPListener(t, serverAddress)
	go func() { clientDone <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		waitForTCPShutdown(t, "client", clientDone)
		waitForTCPShutdown(t, "server", serverDone)
	})

	waitForTCPStatus(t, func() (int, int, int) {
		clientStatusValue, clientErr := client.Status()
		serverStatusValue, serverErr := server.Status()
		if clientErr != nil || serverErr != nil {
			return -1, -1, -1
		}
		clientStatus := clientStatusValue.(control.ClientStatus)
		serverStatus := serverStatusValue.(control.ServerStatus)
		if !clientStatus.FrontendAuthEnabled || !clientStatus.PeerAuthEnabled || !serverStatus.PeerAuthEnabled {
			return -1, -1, -1
		}
		return clientStatus.Sessions, serverStatus.Sessions, serverStatus.Streams
	}, 1, 1, 0)

	assertSOCKS5AuthenticationRejected(t, clientAddress, "client", "wrong-secret")
	waitForTCPStatus(t, func() (int, int, int) {
		clientStatusValue, clientErr := client.Status()
		serverStatusValue, serverErr := server.Status()
		if clientErr != nil || serverErr != nil {
			return -1, -1, -1
		}
		clientStatus := clientStatusValue.(control.ClientStatus)
		serverStatus := serverStatusValue.(control.ServerStatus)
		return clientStatus.Streams, serverStatus.Streams, serverStatus.Sessions
	}, 0, 0, 1)

	assertSOCKS5TaggedEchoWithAuth(t, clientAddress, destinationListener.Addr().String(), "authenticated:", "client", "local-secret")
}

func startTaggedTCPServer(t *testing.T, prefix string) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveTaggedTCPServer(t, listener, prefix)
	return listener
}

func serveTaggedTCPServer(t *testing.T, listener net.Listener, prefix string) {
	t.Helper()
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer conn.Close()
				payload := make([]byte, 4)
				if _, readErr := io.ReadFull(conn, payload); readErr != nil {
					return
				}
				_, _ = conn.Write(append([]byte(prefix), payload...))
			}()
		}
	}()
}

func assertSOCKS5TaggedEcho(t *testing.T, proxyAddress, destinationAddress, prefix string) {
	assertSOCKS5TaggedEchoWithAuth(t, proxyAddress, destinationAddress, prefix, "", "")
}

func assertSOCKS5AuthenticationRejected(t *testing.T, proxyAddress, username, password string) {
	t.Helper()
	conn := dialTCPEventually(t, proxyAddress)
	defer conn.Close()
	if _, err := conn.Write([]byte{5, 1, 2}); err != nil {
		t.Fatal(err)
	}
	methodReply := make([]byte, 2)
	if _, err := io.ReadFull(conn, methodReply); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(methodReply, []byte{5, 2}) {
		t.Fatalf("SOCKS5 method reply = %v, want username/password", methodReply)
	}
	authRequest := append([]byte{1, byte(len(username))}, []byte(username)...)
	authRequest = append(authRequest, byte(len(password)))
	authRequest = append(authRequest, []byte(password)...)
	if _, err := conn.Write(authRequest); err != nil {
		t.Fatal(err)
	}
	authReply := make([]byte, 2)
	if _, err := io.ReadFull(conn, authReply); err != nil {
		t.Fatal(err)
	}
	if authReply[0] != 1 || authReply[1] == 0 {
		t.Fatalf("SOCKS5 auth reply = %v, want failure", authReply)
	}
}

func assertSOCKS5TaggedEchoWithAuth(t *testing.T, proxyAddress, destinationAddress, prefix, username, password string) {
	t.Helper()
	conn := dialSOCKS5EventuallyWithAuth(t, proxyAddress, destinationAddress, username, password)
	defer conn.Close()
	wantPayload := []byte("ping")
	if _, err := conn.Write(wantPayload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(prefix)+len(wantPayload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	want := append([]byte(prefix), wantPayload...)
	if !bytes.Equal(got, want) {
		t.Fatalf("echo = %q, want %q", got, want)
	}
}

func dialSOCKS5Eventually(t *testing.T, proxyAddress, destinationAddress string) net.Conn {
	t.Helper()
	return dialSOCKS5EventuallyWithAuth(t, proxyAddress, destinationAddress, "", "")
}

func dialSOCKS5EventuallyWithAuth(t *testing.T, proxyAddress, destinationAddress, username, password string) net.Conn {
	t.Helper()
	conn := dialTCPEventually(t, proxyAddress)
	negotiateSOCKS5(t, conn, username, password)
	writeSOCKS5Connect(t, conn, destinationAddress)
	if reply := readSOCKS5Reply(t, conn); reply != byte(socks5.ReplySucceeded) {
		_ = conn.Close()
		t.Fatalf("SOCKS5 CONNECT reply = %d, want success", reply)
	}
	return conn
}

func negotiateSOCKS5(t *testing.T, conn net.Conn, username, password string) {
	t.Helper()
	methods := []byte{5, 1, 0}
	if username != "" {
		methods = []byte{5, 2, 0, 2}
	}
	if _, err := conn.Write(methods); err != nil {
		t.Fatal(err)
	}
	methodReply := make([]byte, 2)
	if _, err := io.ReadFull(conn, methodReply); err != nil {
		t.Fatal(err)
	}
	wantMethod := byte(0)
	if username != "" {
		wantMethod = 2
	}
	if !bytes.Equal(methodReply, []byte{5, wantMethod}) {
		t.Fatalf("SOCKS5 method reply = %v", methodReply)
	}
	if username != "" {
		authRequest := append([]byte{1, byte(len(username))}, []byte(username)...)
		authRequest = append(authRequest, byte(len(password)))
		authRequest = append(authRequest, []byte(password)...)
		if _, err := conn.Write(authRequest); err != nil {
			t.Fatal(err)
		}
		authReply := make([]byte, 2)
		if _, err := io.ReadFull(conn, authReply); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(authReply, []byte{1, 0}) {
			t.Fatalf("SOCKS5 auth reply = %v", authReply)
		}
	}
}

func writeSOCKS5Connect(t *testing.T, conn net.Conn, destinationAddress string) {
	t.Helper()
	host, portText, err := net.SplitHostPort(destinationAddress)
	if err != nil {
		t.Fatal(err)
	}
	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		t.Fatal(err)
	}
	request := []byte{5, 1, 0}
	ip := net.ParseIP(host)
	switch {
	case ip != nil && ip.To4() != nil:
		request = append(request, 1)
		request = append(request, ip.To4()...)
	case ip != nil && ip.To16() != nil:
		request = append(request, 4)
		request = append(request, ip.To16()...)
	default:
		hostBytes := []byte(host)
		if len(hostBytes) == 0 || len(hostBytes) > 255 {
			t.Fatalf("invalid SOCKS5 domain destination %q", destinationAddress)
		}
		request = append(request, 3, byte(len(hostBytes)))
		request = append(request, hostBytes...)
	}
	request = append(request, byte(port>>8), byte(port))
	if _, err := conn.Write(request); err != nil {
		t.Fatal(err)
	}
}

func readSOCKS5Reply(t *testing.T, conn net.Conn) byte {
	t.Helper()
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatal(err)
	}
	if header[0] != 5 || header[2] != 0 {
		t.Fatalf("invalid SOCKS5 reply header %v", header)
	}
	addressLength := 0
	switch header[3] {
	case 1:
		addressLength = net.IPv4len
	case 4:
		addressLength = net.IPv6len
	case 3:
		length := []byte{0}
		if _, err := io.ReadFull(conn, length); err != nil {
			t.Fatal(err)
		}
		addressLength = int(length[0])
	default:
		t.Fatalf("invalid SOCKS5 reply address type %d", header[3])
	}
	if _, err := io.ReadFull(conn, make([]byte, addressLength+2)); err != nil {
		t.Fatal(err)
	}
	return header[1]
}

func waitForTCPShutdown(t *testing.T, name string, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("%s Run error: %v", name, err)
		}
	case <-time.After(time.Second):
		t.Errorf("%s did not stop", name)
	}
}

func waitForTCPTraffic(t *testing.T, client *Client, server *serverrole.Server) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		clientStatusValue, clientErr := client.Status()
		serverStatusValue, serverErr := server.Status()
		if clientErr == nil && serverErr == nil {
			clientStatus := clientStatusValue.(control.ClientStatus)
			serverStatus := serverStatusValue.(control.ServerStatus)
			var clientDataBytes, clientControlPackets uint64
			for _, iface := range clientStatus.Interfaces {
				clientDataBytes += iface.Traffic.Data.RXBytes + iface.Traffic.Data.TXBytes
				clientControlPackets += iface.Traffic.Control.RXPackets + iface.Traffic.Control.TXPackets
			}
			var serverDataBytes, serverControlPackets uint64
			for _, socket := range serverStatus.Sockets {
				serverDataBytes += socket.Traffic.Data.RXBytes + socket.Traffic.Data.TXBytes
				serverControlPackets += socket.Traffic.Control.RXPackets + socket.Traffic.Control.TXPackets
			}
			if clientDataBytes > 0 && clientControlPackets > 0 && serverDataBytes > 0 && serverControlPackets > 0 {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("TCP Web traffic counters remained empty")
}

func waitForTCPStatus(t *testing.T, current func() (int, int, int), first, second, third int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		gotFirst, gotSecond, gotThird := current()
		if gotFirst == first && gotSecond == second && gotThird == third {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	gotFirst, gotSecond, gotThird := current()
	t.Fatalf("status = %d/%d/%d, want %d/%d/%d", gotFirst, gotSecond, gotThird, first, second, third)
}

func TestTCPClientStatusIncludesInterfaces(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	client := New(config.Client{
		ListenAddr:         "127.0.0.1:59401",
		DstAddr:            "198.51.100.1:59501",
		Transfer:           transfer,
		IncludeInterfaces:  []string{"active", "idle", "excluded", "br-*"},
		ExcludedInterfaces: []string{"excluded", "br-*"},
		InterfaceLabels:    map[string]string{"active": "Primary"},
	}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Name: "active"}, {Name: "idle"}, {Name: "excluded"}, {Name: "br-docker"}, {Name: "outside-allowlist"}}, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "192.0.2.10" }
	streamID, err := tcpstream.NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	client.setTCPRuntime(&tcpClientRuntime{
		client: client,
		flows:  map[tcpstream.StreamID]*tcpstream.Flow{streamID: nil},
		carriers: map[tcpstream.StreamID]map[string]*tcpstream.Carrier{
			streamID: {"active": {}},
		},
	})

	statusValue, err := client.Status()
	if err != nil {
		t.Fatal(err)
	}
	status := statusValue.(control.ClientStatus)
	if status.Streams != 1 || status.Carriers != 1 || len(status.Interfaces) != 5 {
		t.Fatalf("status = %#v", status)
	}
	if status.Interfaces[0].Status != "active" || status.Interfaces[0].Label != "Primary" {
		t.Fatalf("active interface = %#v", status.Interfaces[0])
	}
	if status.Interfaces[1].Status != "idle" || status.Interfaces[2].Status != "excluded" || status.Interfaces[3].Status != "excluded" || status.Interfaces[4].Status != "excluded" {
		t.Fatalf("interfaces = %#v", status.Interfaces)
	}
	if status.Interfaces[0].DstAddress != "198.51.100.1:59501" || status.Interfaces[1].DstAddress != "198.51.100.1:59501" || status.Interfaces[2].DstAddress != "" || status.Interfaces[3].DstAddress != "" || status.Interfaces[4].DstAddress != "" {
		t.Fatalf("interfaces = %#v", status.Interfaces)
	}

	client.listInterfaces = func() ([]net.Interface, error) { return nil, errors.New("interfaces") }
	if _, err := client.Status(); err == nil {
		t.Fatal("Status succeeded after interface error")
	}
}

func dialTCPEventually(t *testing.T, address string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp4", address, 20*time.Millisecond)
		if err == nil {
			return conn
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("listener %s did not accept a connection", address)
	return nil
}

func freeTCPAddress(t testing.TB) string {
	t.Helper()
	for {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		if _, used := reservedTCPTestAddresses.LoadOrStore(address, struct{}{}); !used {
			return address
		}
	}
}

func waitForTCPListener(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp4", address, 250*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("listener %s did not start", address)
}
