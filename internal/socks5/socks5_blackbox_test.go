package socks5_test

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/socks5"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

type memoryAddr string

func (address memoryAddr) Network() string { return "memory" }
func (address memoryAddr) String() string  { return string(address) }

type memoryConn struct {
	input  *bytes.Reader
	output bytes.Buffer
}

func newMemoryConn(input []byte) *memoryConn {
	return &memoryConn{input: bytes.NewReader(append([]byte(nil), input...))}
}

func (conn *memoryConn) Read(payload []byte) (int, error) {
	// Make every protocol field survive arbitrary TCP fragmentation.
	if len(payload) > 1 {
		payload = payload[:1]
	}
	return conn.input.Read(payload)
}

func (conn *memoryConn) Write(payload []byte) (int, error) {
	return conn.output.Write(payload)
}

func (conn *memoryConn) Close() error                     { return nil }
func (conn *memoryConn) LocalAddr() net.Addr              { return memoryAddr("local") }
func (conn *memoryConn) RemoteAddr() net.Addr             { return memoryAddr("remote") }
func (conn *memoryConn) SetDeadline(time.Time) error      { return nil }
func (conn *memoryConn) SetReadDeadline(time.Time) error  { return nil }
func (conn *memoryConn) SetWriteDeadline(time.Time) error { return nil }
func (conn *memoryConn) written() []byte                  { return append([]byte(nil), conn.output.Bytes()...) }

func TestReadConnectRFCWireNoAuthentication(t *testing.T) {
	tests := []struct {
		name        string
		wire        []byte
		wantAddress string
	}{
		{
			name: "IPv4",
			wire: []byte{
				0x05, 0x01, 0x00,
				0x05, 0x01, 0x00, 0x01, 0xc0, 0x00, 0x02, 0x01, 0x01, 0xbb,
			},
			wantAddress: "192.0.2.1:443",
		},
		{
			name: "domain",
			wire: []byte{
				0x05, 0x02, 0x02, 0x00,
				0x05, 0x01, 0x00, 0x03, 0x0b,
				'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
				0x00, 0x35,
			},
			wantAddress: "example.com:53",
		},
		{
			name: "IPv6",
			wire: []byte{
				0x05, 0x01, 0x00,
				0x05, 0x01, 0x00, 0x04,
				0x20, 0x01, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				0x20, 0xfb,
			},
			wantAddress: "[2001:db8::1]:8443",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := newMemoryConn(test.wire)
			destination, err := socks5.ReadConnect(conn, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if got := destination.String(); got != test.wantAddress {
				t.Fatalf("destination = %q, want %q", got, test.wantAddress)
			}
			if got, want := conn.written(), []byte{0x05, 0x00}; !bytes.Equal(got, want) {
				t.Fatalf("wire reply = % x, want % x", got, want)
			}
		})
	}
}

func TestReadConnectRFC1929Wire(t *testing.T) {
	conn := newMemoryConn([]byte{
		0x05, 0x02, 0x00, 0x02,
		0x01, 0x06, 'c', 'l', 'i', 'e', 'n', 't', 0x06, 's', 'e', 'c', 'r', 'e', 't',
		0x05, 0x01, 0x00, 0x01, 0xcb, 0x00, 0x71, 0x08, 0x04, 0x38,
	})
	destination, err := socks5.ReadConnectWithAuth(conn, time.Second, &socks5.Credentials{
		Username: "client",
		Password: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := destination.String(); got != "203.0.113.8:1080" {
		t.Fatalf("destination = %q, want %q", got, "203.0.113.8:1080")
	}
	if got, want := conn.written(), []byte{0x05, 0x02, 0x01, 0x00}; !bytes.Equal(got, want) {
		t.Fatalf("wire reply = % x, want % x", got, want)
	}
}

func TestReadConnectRFCWireNoAcceptableMethod(t *testing.T) {
	conn := newMemoryConn([]byte{0x05, 0x01, 0x02})
	if _, err := socks5.ReadConnect(conn, time.Second); !errors.Is(err, socks5.ErrNoAcceptableMethod) {
		t.Fatalf("ReadConnect error = %v, want %v", err, socks5.ErrNoAcceptableMethod)
	}
	if got, want := conn.written(), []byte{0x05, 0xff}; !bytes.Equal(got, want) {
		t.Fatalf("wire reply = % x, want % x", got, want)
	}
}

func TestReadConnectRFCWireUnsupportedRequest(t *testing.T) {
	tests := []struct {
		name    string
		wire    []byte
		wantErr error
		want    []byte
	}{
		{
			name: "BIND",
			wire: []byte{
				0x05, 0x01, 0x00,
				0x05, 0x02, 0x00, 0x01, 0x7f, 0x00, 0x00, 0x01, 0x04, 0x38,
			},
			wantErr: socks5.ErrCommandNotSupported,
			want:    []byte{0x05, 0x00, 0x05, 0x07, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name: "UDP ASSOCIATE",
			wire: []byte{
				0x05, 0x01, 0x00,
				0x05, 0x03, 0x00, 0x01, 0x7f, 0x00, 0x00, 0x01, 0x04, 0x38,
			},
			wantErr: socks5.ErrCommandNotSupported,
			want:    []byte{0x05, 0x00, 0x05, 0x07, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name: "unknown address type",
			wire: []byte{
				0x05, 0x01, 0x00,
				0x05, 0x01, 0x00, 0x7f,
			},
			wantErr: socks5.ErrInvalidRequest,
			want:    []byte{0x05, 0x00, 0x05, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := newMemoryConn(test.wire)
			if _, err := socks5.ReadConnect(conn, time.Second); !errors.Is(err, test.wantErr) {
				t.Fatalf("ReadConnect error = %v, want %v", err, test.wantErr)
			}
			if got := conn.written(); !bytes.Equal(got, test.want) {
				t.Fatalf("wire reply = % x, want % x", got, test.want)
			}
		})
	}
}

func TestWriteReplyRFCWireValues(t *testing.T) {
	tests := []struct {
		name  string
		reply socks5.Reply
		want  []byte
	}{
		{name: "succeeded", reply: socks5.ReplySucceeded, want: []byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "general failure", reply: socks5.ReplyGeneralFailure, want: []byte{0x05, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "not allowed", reply: socks5.ReplyNotAllowed, want: []byte{0x05, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "network unreachable", reply: socks5.ReplyNetworkUnreachable, want: []byte{0x05, 0x03, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "host unreachable", reply: socks5.ReplyHostUnreachable, want: []byte{0x05, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "connection refused", reply: socks5.ReplyConnectionRefused, want: []byte{0x05, 0x05, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "TTL expired", reply: socks5.ReplyTTLExpired, want: []byte{0x05, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "command not supported", reply: socks5.ReplyCommandNotSupported, want: []byte{0x05, 0x07, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "address not supported", reply: socks5.ReplyAddressNotSupported, want: []byte{0x05, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := newMemoryConn(nil)
			if err := socks5.WriteReply(conn, test.reply, time.Second); err != nil {
				t.Fatal(err)
			}
			if got := conn.written(); !bytes.Equal(got, test.want) {
				t.Fatalf("wire reply = % x, want % x", got, test.want)
			}
		})
	}
}

func TestReplyForErrorRFCWireValues(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want []byte
	}{
		{name: "generic", err: errors.New("open failed"), want: []byte{0x05, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "general failure", err: &tcpstream.OpenError{Result: tcpstream.OpenResultGeneralFailure}, want: []byte{0x05, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "policy denied", err: &tcpstream.OpenError{Result: tcpstream.OpenResultPolicyDenied}, want: []byte{0x05, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "network unreachable", err: &tcpstream.OpenError{Result: tcpstream.OpenResultNetworkUnreachable}, want: []byte{0x05, 0x03, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "host unreachable", err: &tcpstream.OpenError{Result: tcpstream.OpenResultHostUnreachable}, want: []byte{0x05, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "wrapped connection refused", err: fmt.Errorf("wrapped: %w", &tcpstream.OpenError{Result: tcpstream.OpenResultConnectionRefused}), want: []byte{0x05, 0x05, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "timeout", err: &tcpstream.OpenError{Result: tcpstream.OpenResultTimeout}, want: []byte{0x05, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := newMemoryConn(nil)
			if err := socks5.WriteReply(conn, socks5.ReplyForError(test.err), time.Second); err != nil {
				t.Fatal(err)
			}
			if got := conn.written(); !bytes.Equal(got, test.want) {
				t.Fatalf("wire reply = % x, want % x", got, test.want)
			}
		})
	}
}
