package tcpbind

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

const tcpbindTestTimeout = 5 * time.Second

func TestDialContextUsesDestinationAndSourceIPv4(t *testing.T) {
	listener := newTCPBindTestListener(t)
	accepted := acceptTCPBindTestConnection(listener)

	ctx, cancel := context.WithTimeout(context.Background(), tcpbindTestTimeout)
	defer cancel()
	conn, err := DialContext(ctx, listener.Addr().String(), "127.0.0.1", "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(tcpbindTestTimeout)); err != nil {
		t.Fatal(err)
	}
	local, ok := conn.LocalAddr().(*net.TCPAddr)
	if !ok || !local.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("local address = %#v, want source IP 127.0.0.1", conn.LocalAddr())
	}
	if conn.RemoteAddr().String() != listener.Addr().String() {
		t.Fatalf("remote address = %s, want %s", conn.RemoteAddr(), listener.Addr())
	}
	peer := waitForTCPBindTestAccept(t, accepted)
	t.Cleanup(func() { _ = peer.Close() })
	if err := peer.SetDeadline(time.Now().Add(tcpbindTestTimeout)); err != nil {
		t.Fatal(err)
	}
	if peer.LocalAddr().String() != listener.Addr().String() {
		t.Fatalf("accepted local address = %s, want %s", peer.LocalAddr(), listener.Addr())
	}
	if acceptedRemote, ok := peer.RemoteAddr().(*net.TCPAddr); !ok || !acceptedRemote.IP.Equal(local.IP) || acceptedRemote.Port != local.Port {
		t.Fatalf("accepted remote address = %#v, want %#v", peer.RemoteAddr(), local)
	}

	if _, err := conn.Write([]byte{0x5a}); err != nil {
		t.Fatal(err)
	}
	var payload [1]byte
	if _, err := io.ReadFull(peer, payload[:]); err != nil {
		t.Fatal(err)
	}
	if payload[0] != 0x5a {
		t.Fatalf("accepted payload = %x, want 5a", payload[0])
	}
}

func TestDialContextHonorsCanceledContext(t *testing.T) {
	listener := newTCPBindTestListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, err := DialContext(ctx, listener.Addr().String(), "127.0.0.1", "", time.Second)
	if conn != nil {
		_ = conn.Close()
		t.Fatal("DialContext returned a connection for a canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DialContext error = %v, want context.Canceled", err)
	}
}

func TestDialContextHonorsExpiredTimeout(t *testing.T) {
	listener := newTCPBindTestListener(t)
	conn, err := DialContext(context.Background(), listener.Addr().String(), "127.0.0.1", "", -time.Nanosecond)
	if conn != nil {
		_ = conn.Close()
		t.Fatal("DialContext returned a connection with an expired timeout")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("DialContext error = %v, want a timeout", err)
	}
}

func newTCPBindTestListener(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.SetDeadline(time.Now().Add(tcpbindTestTimeout)); err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func acceptTCPBindTestConnection(listener *net.TCPListener) <-chan net.Conn {
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := listener.Accept()
		accepted <- conn
	}()
	return accepted
}

func waitForTCPBindTestAccept(t *testing.T, accepted <-chan net.Conn) net.Conn {
	t.Helper()
	select {
	case conn := <-accepted:
		if conn == nil {
			t.Fatal("TCP listener stopped before accepting the connection")
		}
		return conn
	case <-time.After(tcpbindTestTimeout):
		t.Fatal("timed out waiting for TCP listener accept")
		return nil
	}
}
