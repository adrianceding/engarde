package socks5

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/adrianceding/engarde/internal/tcpstream"
)

func FuzzReadConnect(f *testing.F) {
	credentials := &Credentials{Username: "client", Password: "secret"}
	authRequest := []byte{authVersion, byte(len(credentials.Username))}
	authRequest = append(authRequest, credentials.Username...)
	authRequest = append(authRequest, byte(len(credentials.Password)))
	authRequest = append(authRequest, credentials.Password...)

	maximumMethods := append([]byte{version5, 255}, bytes.Repeat([]byte{0x80}, 254)...)
	maximumMethods = append(maximumMethods, methodNoAuth)
	maximumMethods = append(maximumMethods, connectRequest(addressIPv4, []byte{192, 0, 2, 1}, 443)...)
	maximumWireDomain := strings.Repeat("a", 255)

	seeds := []struct {
		input   []byte
		useAuth bool
	}{
		{
			input: noAuthInput(connectRequest(addressIPv4, []byte{192, 0, 2, 1}, 443)),
		},
		{
			input: noAuthInput(connectRequest(addressIPv6, []byte{
				0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 1,
			}, 8443)),
		},
		{
			input: noAuthInput(connectRequest(addressDomain, domainAddress("proxy.example.com"), 8388)),
		},
		{
			input:   authInput(authRequest, connectRequest(addressIPv4, []byte{198, 51, 100, 4}, 443)),
			useAuth: true,
		},
		{
			input: noAuthInput([]byte{version5, commandConnect, 0, 0xff}),
		},
		{
			input: []byte{version5, 255},
		},
		{
			input: maximumMethods,
		},
		{
			input: noAuthInput(connectRequest(addressDomain, domainAddress(maximumWireDomain), 443)),
		},
		{
			input: noAuthInput([]byte{version5, commandConnect, 1, addressIPv4}),
		},
		{
			input: noAuthInput([]byte{version5, 2, 0, addressIPv4}),
		},
		{
			input:   authInput([]byte{2, 1, 'x', 1, 'y'}, nil),
			useAuth: true,
		},
		{
			input: noAuthInput([]byte{version5, commandConnect, 0, addressDomain, 255, 'a'}),
		},
		{},
	}
	for _, seed := range seeds {
		f.Add(seed.input, seed.useAuth)
	}

	f.Fuzz(func(t *testing.T, input []byte, useAuth bool) {
		conn, output := newScriptedConn(input)
		conn.writer = &chunkWriter{writer: output, max: 1}
		var destination tcpstream.Destination
		var err error
		if useAuth {
			destination, err = ReadConnectWithAuth(conn, 0, credentials)
		} else {
			destination, err = ReadConnect(conn, 0)
		}

		if err == nil {
			if destination.IsZero() {
				t.Fatal("successful handshake returned a zero destination")
			}
			if _, encodeErr := destination.Encode(); encodeErr != nil {
				t.Fatalf("successful destination cannot be encoded: %v", encodeErr)
			}
		} else if !destination.IsZero() {
			t.Fatalf("failed handshake returned destination %q", destination.String())
		}
		if output.Len() > 14 {
			t.Fatalf("handshake wrote %d bytes, want at most 14", output.Len())
		}
		if len(conn.deadlineCalls) != 0 {
			t.Fatalf("timeout=0 changed connection deadline: %v", conn.deadlineCalls)
		}
		if len(conn.writeDeadlineCalls) != 0 {
			t.Fatalf("timeout=0 changed write deadline: %v", conn.writeDeadlineCalls)
		}
	})
}

func FuzzReplyForError(f *testing.F) {
	for _, result := range []byte{0, 1, 2, 3, 4, 5, 6, 0xff} {
		for wrapStyle := byte(0); wrapStyle < 5; wrapStyle++ {
			f.Add(result, wrapStyle)
		}
	}

	f.Fuzz(func(t *testing.T, resultByte, wrapStyle byte) {
		openError := &tcpstream.OpenError{Result: tcpstream.OpenResult(resultByte)}
		var err error
		hasOpenError := true
		switch wrapStyle % 5 {
		case 0:
			err = openError
		case 1:
			err = fmt.Errorf("wrapped: %w", openError)
		case 2:
			err = &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("destination: %w", openError)}
		case 3:
			err = errors.Join(errors.New("carrier failed"), fmt.Errorf("destination: %w", openError))
		default:
			err = &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection failed")}
			hasOpenError = false
		}

		want := ReplyGeneralFailure
		if hasOpenError {
			switch tcpstream.OpenResult(resultByte) {
			case tcpstream.OpenResultConnectionRefused:
				want = ReplyConnectionRefused
			case tcpstream.OpenResultNetworkUnreachable:
				want = ReplyNetworkUnreachable
			case tcpstream.OpenResultHostUnreachable:
				want = ReplyHostUnreachable
			case tcpstream.OpenResultTimeout:
				want = ReplyTTLExpired
			case tcpstream.OpenResultPolicyDenied:
				want = ReplyNotAllowed
			}
		}
		if got := ReplyForError(err); got != want {
			t.Fatalf("ReplyForError(%T, result=%d, style=%d) = %d, want %d", err, resultByte, wrapStyle, got, want)
		}
	})
}
