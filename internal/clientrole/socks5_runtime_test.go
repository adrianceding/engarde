package clientrole

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
)

type runtimeSOCKS5Harness struct {
	client  *Client
	address string
}

func startRuntimeSOCKS5Harness(t *testing.T, credentials *config.Credentials) runtimeSOCKS5Harness {
	t.Helper()
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.DialTimeoutMillis = 50
	transfer.TCP.OpenTimeoutMillis = 50
	address := freeTCPAddress(t)
	client := New(config.Client{
		ListenAddr: address,
		DstAddr:    "127.0.0.1:1",
		SOCKS5Auth: credentials,
		Transfer:   transfer,
	}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) { return nil, nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	waitForTCPListener(t, address)
	t.Cleanup(func() {
		cancel()
		waitForTCPShutdown(t, "client", done)
	})
	return runtimeSOCKS5Harness{client: client, address: address}
}

func TestTCPSOCKS5RuntimeRejectsInvalidNegotiationAndRequests(t *testing.T) {
	harness := startRuntimeSOCKS5Harness(t, nil)
	tests := []struct {
		name      string
		negotiate bool
		request   []byte
		want      []byte
	}{
		{name: "invalid greeting version", request: []byte{4, 1, 0}},
		{name: "no acceptable method", request: []byte{5, 1, 2}, want: []byte{5, 0xff}},
		{name: "BIND", negotiate: true, request: []byte{5, 2, 0, 1}, want: []byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0}},
		{name: "UDP ASSOCIATE", negotiate: true, request: []byte{5, 3, 0, 1}, want: []byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0}},
		{name: "unknown address type", negotiate: true, request: []byte{5, 1, 0, 0x7f}, want: []byte{5, 8, 0, 1, 0, 0, 0, 0, 0, 0}},
		{name: "nonzero reserved byte", negotiate: true, request: []byte{5, 1, 1, 1}, want: []byte{5, 1, 0, 1, 0, 0, 0, 0, 0, 0}},
		{name: "empty domain", negotiate: true, request: []byte{5, 1, 0, 3, 0}, want: []byte{5, 1, 0, 1, 0, 0, 0, 0, 0, 0}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := dialRuntimeSOCKS5(t, harness.address)
			defer conn.Close()
			if test.negotiate {
				negotiateRuntimeSOCKS5NoAuth(t, conn)
			}
			writeRuntimeSOCKS5(t, conn, test.request)
			if len(test.want) > 0 {
				readRuntimeSOCKS5Exact(t, conn, test.want)
			}
			assertRuntimeSOCKS5Closed(t, conn)
			waitForRuntimeSOCKS5Idle(t, harness.client)
		})
	}
}

func TestTCPSOCKS5RuntimeReturnsGeneralFailureWithoutCarriers(t *testing.T) {
	harness := startRuntimeSOCKS5Harness(t, nil)
	conn := dialRuntimeSOCKS5(t, harness.address)
	defer conn.Close()
	negotiateRuntimeSOCKS5NoAuth(t, conn)
	writeRuntimeSOCKS5(t, conn, []byte{5, 1, 0, 1, 127, 0, 0, 1, 0, 80})
	readRuntimeSOCKS5Exact(t, conn, []byte{5, 1, 0, 1, 0, 0, 0, 0, 0, 0})
	assertRuntimeSOCKS5Closed(t, conn)
	waitForRuntimeSOCKS5Idle(t, harness.client)
}

func TestTCPSOCKS5RuntimeAuthenticationFailuresCloseAndRecover(t *testing.T) {
	harness := startRuntimeSOCKS5Harness(t, &config.Credentials{Username: "u", Password: "p"})

	t.Run("no username password method", func(t *testing.T) {
		conn := dialRuntimeSOCKS5(t, harness.address)
		defer conn.Close()
		writeRuntimeSOCKS5(t, conn, []byte{5, 1, 0})
		readRuntimeSOCKS5Exact(t, conn, []byte{5, 0xff})
		assertRuntimeSOCKS5Closed(t, conn)
		waitForRuntimeSOCKS5Idle(t, harness.client)
	})

	t.Run("wrong password", func(t *testing.T) {
		conn := dialRuntimeSOCKS5(t, harness.address)
		defer conn.Close()
		negotiateRuntimeSOCKS5Password(t, conn)
		writeRuntimeSOCKS5(t, conn, []byte{1, 1, 'u', 1, 'x'})
		readRuntimeSOCKS5Exact(t, conn, []byte{1, 1})
		assertRuntimeSOCKS5Closed(t, conn)
		waitForRuntimeSOCKS5Idle(t, harness.client)
	})

	t.Run("valid authentication after failures", func(t *testing.T) {
		conn := dialRuntimeSOCKS5(t, harness.address)
		defer conn.Close()
		negotiateRuntimeSOCKS5Password(t, conn)
		writeRuntimeSOCKS5(t, conn, []byte{1, 1, 'u', 1, 'p'})
		readRuntimeSOCKS5Exact(t, conn, []byte{1, 0})
		writeRuntimeSOCKS5(t, conn, []byte{5, 2, 0, 1})
		readRuntimeSOCKS5Exact(t, conn, []byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0})
		assertRuntimeSOCKS5Closed(t, conn)
		waitForRuntimeSOCKS5Idle(t, harness.client)
	})
}

func TestTCPSOCKS5RuntimeTimesOutPartialHandshakes(t *testing.T) {
	harness := startRuntimeSOCKS5Harness(t, nil)
	tests := []struct {
		name      string
		negotiate bool
		partial   []byte
	}{
		{name: "method list", partial: []byte{5, 2, 0}},
		{name: "request header", negotiate: true, partial: []byte{5, 1}},
		{name: "domain", negotiate: true, partial: []byte{5, 1, 0, 3, 4, 't', 'e'}},
		{name: "port", negotiate: true, partial: []byte{5, 1, 0, 1, 127, 0, 0, 1, 0}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := dialRuntimeSOCKS5(t, harness.address)
			defer conn.Close()
			if test.negotiate {
				negotiateRuntimeSOCKS5NoAuth(t, conn)
			}
			writeRuntimeSOCKS5(t, conn, test.partial)
			assertRuntimeSOCKS5Closed(t, conn)
			waitForRuntimeSOCKS5Idle(t, harness.client)
		})
	}
}

func dialRuntimeSOCKS5(t *testing.T, address string) net.Conn {
	t.Helper()
	conn := dialTCPEventually(t, address)
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	return conn
}

func negotiateRuntimeSOCKS5NoAuth(t *testing.T, conn net.Conn) {
	t.Helper()
	writeRuntimeSOCKS5(t, conn, []byte{5, 1, 0})
	readRuntimeSOCKS5Exact(t, conn, []byte{5, 0})
}

func negotiateRuntimeSOCKS5Password(t *testing.T, conn net.Conn) {
	t.Helper()
	writeRuntimeSOCKS5(t, conn, []byte{5, 1, 2})
	readRuntimeSOCKS5Exact(t, conn, []byte{5, 2})
}

func writeRuntimeSOCKS5(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
}

func readRuntimeSOCKS5Exact(t *testing.T, conn net.Conn, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("SOCKS5 wire reply = %v, want %v", got, want)
	}
}

func assertRuntimeSOCKS5Closed(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	read, err := conn.Read(buffer)
	if read != 0 || err == nil {
		t.Fatalf("SOCKS5 connection remained open or returned trailing data: read=%d err=%v data=%v", read, err, buffer[:read])
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("SOCKS5 peer did not actively close the connection: %v", err)
	}
}

func waitForRuntimeSOCKS5Idle(t *testing.T, client *Client) {
	t.Helper()
	waitForTCPRuntimeCondition(t, func() bool {
		runtime := client.getTCPRuntime()
		if runtime == nil {
			return false
		}
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return len(runtime.accepted) == 0 && len(runtime.flows) == 0 && len(runtime.groups) == 0 && len(runtime.sessions) == len(runtime.paths)
	})
}
