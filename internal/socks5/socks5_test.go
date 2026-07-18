package socks5

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/tcpstream"
)

type testAddr string

func (address testAddr) Network() string { return "test" }
func (address testAddr) String() string  { return string(address) }

type scriptedConn struct {
	reader io.Reader
	writer io.Writer

	deadlineCalls      []time.Time
	writeDeadlineCalls []time.Time
	setDeadlineFunc    func(time.Time) error
	setWriteDeadlineFn func(time.Time) error
}

func (conn *scriptedConn) Read(payload []byte) (int, error) {
	if conn.reader == nil {
		return 0, io.EOF
	}
	return conn.reader.Read(payload)
}

func (conn *scriptedConn) Write(payload []byte) (int, error) {
	if conn.writer == nil {
		return len(payload), nil
	}
	return conn.writer.Write(payload)
}

func (conn *scriptedConn) Close() error                    { return nil }
func (conn *scriptedConn) LocalAddr() net.Addr             { return testAddr("local") }
func (conn *scriptedConn) RemoteAddr() net.Addr            { return testAddr("remote") }
func (conn *scriptedConn) SetReadDeadline(time.Time) error { return nil }
func (conn *scriptedConn) SetDeadline(deadline time.Time) error {
	conn.deadlineCalls = append(conn.deadlineCalls, deadline)
	if conn.setDeadlineFunc != nil {
		return conn.setDeadlineFunc(deadline)
	}
	return nil
}

func (conn *scriptedConn) SetWriteDeadline(deadline time.Time) error {
	conn.writeDeadlineCalls = append(conn.writeDeadlineCalls, deadline)
	if conn.setWriteDeadlineFn != nil {
		return conn.setWriteDeadlineFn(deadline)
	}
	return nil
}

type chunkReader struct {
	reader io.Reader
	max    int
}

func (reader *chunkReader) Read(payload []byte) (int, error) {
	if len(payload) > reader.max {
		payload = payload[:reader.max]
	}
	return reader.reader.Read(payload)
}

type chunkWriter struct {
	writer io.Writer
	max    int
}

func (writer *chunkWriter) Write(payload []byte) (int, error) {
	if len(payload) > writer.max {
		payload = payload[:writer.max]
	}
	return writer.writer.Write(payload)
}

type errorWriter struct {
	err error
}

func (writer errorWriter) Write([]byte) (int, error) {
	return 0, writer.err
}

type errorReader struct {
	err error
}

func (reader errorReader) Read([]byte) (int, error) {
	return 0, reader.err
}

type failAfterWriter struct {
	writer    io.Writer
	remaining int
	err       error
}

func (writer *failAfterWriter) Write(payload []byte) (int, error) {
	if writer.remaining == 0 {
		return 0, writer.err
	}
	if len(payload) > writer.remaining {
		payload = payload[:writer.remaining]
	}
	written, err := writer.writer.Write(payload)
	writer.remaining -= written
	return written, err
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) {
	return 0, nil
}

func newScriptedConn(input []byte) (*scriptedConn, *bytes.Buffer) {
	output := &bytes.Buffer{}
	return &scriptedConn{
		reader: &chunkReader{reader: bytes.NewReader(input), max: 1},
		writer: output,
	}, output
}

func readConnectScript(input []byte, credentials *Credentials) (tcpstream.Destination, error, []byte, *scriptedConn) {
	conn, output := newScriptedConn(input)
	var destination tcpstream.Destination
	var err error
	if credentials == nil {
		destination, err = ReadConnect(conn, time.Second)
	} else {
		destination, err = ReadConnectWithAuth(conn, time.Second, credentials)
	}
	return destination, err, append([]byte(nil), output.Bytes()...), conn
}

func noAuthInput(request []byte) []byte {
	input := []byte{version5, 3, 0x80, methodUsernamePassword, methodNoAuth}
	return append(input, request...)
}

func authInput(authRequest, connectRequest []byte) []byte {
	input := []byte{version5, 2, methodNoAuth, methodUsernamePassword}
	input = append(input, authRequest...)
	return append(input, connectRequest...)
}

func connectRequest(addressType byte, address []byte, port uint16) []byte {
	request := []byte{version5, commandConnect, 0, addressType}
	request = append(request, address...)
	return append(request, byte(port>>8), byte(port))
}

func domainAddress(host string) []byte {
	return append([]byte{byte(len(host))}, []byte(host)...)
}

func replyPayload(reply Reply) []byte {
	return []byte{version5, byte(reply), 0, addressIPv4, 0, 0, 0, 0, 0, 0}
}

func assertDeadlineCleared(t *testing.T, calls []time.Time) {
	t.Helper()
	if len(calls) != 2 {
		t.Fatalf("deadline calls = %v, want set and clear", calls)
	}
	if calls[0].IsZero() {
		t.Fatal("first deadline was not set")
	}
	if !calls[1].IsZero() {
		t.Fatalf("final deadline = %v, want zero", calls[1])
	}
}

func TestReadConnectNoAuthentication(t *testing.T) {
	tests := []struct {
		name        string
		request     []byte
		wantType    tcpstream.DestinationType
		wantHost    string
		wantPort    uint16
		wantAddress string
	}{
		{
			name:        "IPv4",
			request:     connectRequest(addressIPv4, []byte{192, 0, 2, 1}, 443),
			wantType:    tcpstream.DestinationIPv4,
			wantHost:    "192.0.2.1",
			wantPort:    443,
			wantAddress: "192.0.2.1:443",
		},
		{
			name: "IPv6",
			request: connectRequest(addressIPv6, []byte{
				0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 1,
			}, 8443),
			wantType:    tcpstream.DestinationIPv6,
			wantHost:    "2001:db8::1",
			wantPort:    8443,
			wantAddress: "[2001:db8::1]:8443",
		},
		{
			name:        "domain",
			request:     connectRequest(addressDomain, domainAddress("Proxy.Example.COM."), 8388),
			wantType:    tcpstream.DestinationDomain,
			wantHost:    "proxy.example.com",
			wantPort:    8388,
			wantAddress: "proxy.example.com:8388",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			destination, err, output, conn := readConnectScript(noAuthInput(test.request), nil)
			if err != nil {
				t.Fatal(err)
			}
			if destination.Type() != test.wantType || destination.Host() != test.wantHost || destination.Port() != test.wantPort {
				t.Fatalf("destination = %q (type %d), want %q (type %d)", destination.String(), destination.Type(), test.wantAddress, test.wantType)
			}
			if destination.String() != test.wantAddress {
				t.Fatalf("destination.String() = %q, want %q", destination.String(), test.wantAddress)
			}
			if want := []byte{version5, methodNoAuth}; !bytes.Equal(output, want) {
				t.Fatalf("output = %v, want %v", output, want)
			}
			assertDeadlineCleared(t, conn.deadlineCalls)
		})
	}
}

func TestReadConnectNonPositiveTimeoutPreservesDeadline(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{name: "zero", timeout: 0},
		{name: "negative", timeout: -time.Second},
	}
	request := connectRequest(addressIPv4, []byte{192, 0, 2, 1}, 443)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, output := newScriptedConn(noAuthInput(request))
			destination, err := ReadConnect(conn, test.timeout)
			if err != nil {
				t.Fatal(err)
			}
			if destination.String() != "192.0.2.1:443" {
				t.Fatalf("destination = %q, want 192.0.2.1:443", destination.String())
			}
			if want := []byte{version5, methodNoAuth}; !bytes.Equal(output.Bytes(), want) {
				t.Fatalf("output = %v, want %v", output.Bytes(), want)
			}
			if len(conn.deadlineCalls) != 0 {
				t.Fatalf("deadline calls = %v, want none", conn.deadlineCalls)
			}
		})
	}
}

func TestReadConnectMaximumMethodList(t *testing.T) {
	t.Run("required method last", func(t *testing.T) {
		methods := bytes.Repeat([]byte{0x80}, 254)
		methods = append(methods, methodNoAuth)
		input := append([]byte{version5, 255}, methods...)
		input = append(input, connectRequest(addressIPv4, []byte{192, 0, 2, 1}, 443)...)

		destination, err, output, _ := readConnectScript(input, nil)
		if err != nil {
			t.Fatal(err)
		}
		if destination.String() != "192.0.2.1:443" {
			t.Fatalf("destination = %q, want 192.0.2.1:443", destination.String())
		}
		if want := []byte{version5, methodNoAuth}; !bytes.Equal(output, want) {
			t.Fatalf("output = %v, want %v", output, want)
		}
	})

	t.Run("all methods unsupported", func(t *testing.T) {
		methods := bytes.Repeat([]byte{0x80}, 255)
		input := append([]byte{version5, 255}, methods...)
		_, err, output, _ := readConnectScript(input, nil)
		if !errors.Is(err, ErrNoAcceptableMethod) {
			t.Fatalf("ReadConnect error = %v, want %v", err, ErrNoAcceptableMethod)
		}
		if want := []byte{version5, methodUnavailable}; !bytes.Equal(output, want) {
			t.Fatalf("output = %v, want %v", output, want)
		}
	})
}

func TestReadConnectMethodNegotiation(t *testing.T) {
	credentials := &Credentials{Username: "client", Password: "secret"}
	tests := []struct {
		name        string
		input       []byte
		credentials *Credentials
		wantErr     error
		wantOutput  []byte
	}{
		{name: "invalid version", input: []byte{4, 1, methodNoAuth}, wantErr: ErrInvalidRequest},
		{name: "zero methods", input: []byte{version5, 0}, wantErr: ErrInvalidRequest},
		{name: "short greeting", input: []byte{version5}, wantErr: io.ErrUnexpectedEOF},
		{name: "short methods", input: []byte{version5, 2, methodNoAuth}, wantErr: io.ErrUnexpectedEOF},
		{
			name:       "no authentication unavailable",
			input:      []byte{version5, 1, methodUsernamePassword},
			wantErr:    ErrNoAcceptableMethod,
			wantOutput: []byte{version5, methodUnavailable},
		},
		{
			name:        "username password unavailable",
			input:       []byte{version5, 2, methodNoAuth, 0x80},
			credentials: credentials,
			wantErr:     ErrNoAcceptableMethod,
			wantOutput:  []byte{version5, methodUnavailable},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			destination, err, output, _ := readConnectScript(test.input, test.credentials)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ReadConnect error = %v, want %v", err, test.wantErr)
			}
			if !destination.IsZero() {
				t.Fatalf("destination = %q, want zero", destination.String())
			}
			if !bytes.Equal(output, test.wantOutput) {
				t.Fatalf("output = %v, want %v", output, test.wantOutput)
			}
		})
	}
}

func TestReadConnectUsernamePassword(t *testing.T) {
	credentials := &Credentials{Username: "client", Password: "local-secret"}
	authRequest := []byte{authVersion, byte(len(credentials.Username))}
	authRequest = append(authRequest, credentials.Username...)
	authRequest = append(authRequest, byte(len(credentials.Password)))
	authRequest = append(authRequest, credentials.Password...)
	request := connectRequest(addressIPv4, []byte{198, 51, 100, 4}, 443)

	destination, err, output, conn := readConnectScript(authInput(authRequest, request), credentials)
	if err != nil {
		t.Fatal(err)
	}
	if destination.String() != "198.51.100.4:443" {
		t.Fatalf("destination = %q, want 198.51.100.4:443", destination.String())
	}
	wantOutput := []byte{version5, methodUsernamePassword, authVersion, authSucceeded}
	if !bytes.Equal(output, wantOutput) {
		t.Fatalf("output = %v, want %v", output, wantOutput)
	}
	assertDeadlineCleared(t, conn.deadlineCalls)
}

func TestReadConnectUsernamePasswordWireBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "minimum", value: "u"},
		{name: "maximum ASCII", value: strings.Repeat("u", 255)},
		{name: "maximum UTF-8 bytes", value: strings.Repeat("界", 85)},
	}
	request := connectRequest(addressIPv6, net.ParseIP("2001:db8::1").To16(), 8443)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			length := len([]byte(test.value))
			if length < 1 || length > 255 {
				t.Fatalf("test credential length = %d, want 1..255 bytes", length)
			}
			credentials := &Credentials{
				Username: test.value,
				Password: test.value,
			}
			authRequest := []byte{authVersion, byte(length)}
			authRequest = append(authRequest, credentials.Username...)
			authRequest = append(authRequest, byte(length))
			authRequest = append(authRequest, credentials.Password...)

			destination, err, output, conn := readConnectScript(authInput(authRequest, request), credentials)
			if err != nil {
				t.Fatal(err)
			}
			if destination.Type() != tcpstream.DestinationIPv6 || destination.String() != "[2001:db8::1]:8443" {
				t.Fatalf("destination = %q (type %d), want [2001:db8::1]:8443 (IPv6)", destination.String(), destination.Type())
			}
			wantOutput := []byte{version5, methodUsernamePassword, authVersion, authSucceeded}
			if !bytes.Equal(output, wantOutput) {
				t.Fatalf("output = %v, want %v", output, wantOutput)
			}
			assertDeadlineCleared(t, conn.deadlineCalls)
		})
	}
}

func TestReadConnectCompletesProtocolShortWrites(t *testing.T) {
	credentials := &Credentials{Username: "client", Password: "secret"}
	authRequest := []byte{authVersion, byte(len(credentials.Username))}
	authRequest = append(authRequest, credentials.Username...)
	authRequest = append(authRequest, byte(len(credentials.Password)))
	authRequest = append(authRequest, credentials.Password...)

	tests := []struct {
		name            string
		input           []byte
		credentials     *Credentials
		wantDestination string
		wantErr         error
		wantOutput      []byte
	}{
		{
			name:            "no authentication selection",
			input:           noAuthInput(connectRequest(addressIPv4, []byte{192, 0, 2, 1}, 443)),
			wantDestination: "192.0.2.1:443",
			wantOutput:      []byte{version5, methodNoAuth},
		},
		{
			name:            "username password selection and status",
			input:           authInput(authRequest, connectRequest(addressIPv4, []byte{198, 51, 100, 4}, 8443)),
			credentials:     credentials,
			wantDestination: "198.51.100.4:8443",
			wantOutput:      []byte{version5, methodUsernamePassword, authVersion, authSucceeded},
		},
		{
			name:       "unavailable method response",
			input:      []byte{version5, 1, methodUsernamePassword},
			wantErr:    ErrNoAcceptableMethod,
			wantOutput: []byte{version5, methodUnavailable},
		},
		{
			name:       "invalid request response",
			input:      noAuthInput([]byte{version5, commandConnect, 1, addressIPv4}),
			wantErr:    ErrInvalidRequest,
			wantOutput: append([]byte{version5, methodNoAuth}, replyPayload(ReplyGeneralFailure)...),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, output := newScriptedConn(test.input)
			conn.writer = &chunkWriter{writer: output, max: 1}
			var destination tcpstream.Destination
			var err error
			if test.credentials == nil {
				destination, err = ReadConnect(conn, time.Second)
			} else {
				destination, err = ReadConnectWithAuth(conn, time.Second, test.credentials)
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ReadConnect error = %v, want %v", err, test.wantErr)
			}
			if got := destination.String(); got != test.wantDestination {
				t.Fatalf("destination = %q, want %q", got, test.wantDestination)
			}
			if got := output.Bytes(); !bytes.Equal(got, test.wantOutput) {
				t.Fatalf("output = %v, want %v", got, test.wantOutput)
			}
		})
	}
}

func TestReadConnectStagedPreservesEarlyData(t *testing.T) {
	credentials := &Credentials{Username: "client", Password: "secret"}
	authRequest := []byte{authVersion, byte(len(credentials.Username))}
	authRequest = append(authRequest, credentials.Username...)
	authRequest = append(authRequest, byte(len(credentials.Password)))
	authRequest = append(authRequest, credentials.Password...)

	tests := []struct {
		name            string
		credentials     *Credentials
		greeting        []byte
		authRequest     []byte
		request         []byte
		wantDestination string
		earlyData       []byte
	}{
		{
			name:            "no authentication domain",
			greeting:        []byte{version5, 2, methodUsernamePassword, methodNoAuth},
			request:         connectRequest(addressDomain, domainAddress("proxy.example.com"), 443),
			wantDestination: "proxy.example.com:443",
			earlyData:       []byte("early no-auth payload"),
		},
		{
			name:            "username password IPv6",
			credentials:     credentials,
			greeting:        []byte{version5, 2, methodNoAuth, methodUsernamePassword},
			authRequest:     authRequest,
			request:         connectRequest(addressIPv6, net.ParseIP("2001:db8::1").To16(), 8443),
			wantDestination: "[2001:db8::1]:8443",
			earlyData:       []byte("early authenticated payload"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()
			if err := clientConn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}

			type result struct {
				destination tcpstream.Destination
				earlyData   []byte
				err         error
			}
			results := make(chan result, 1)
			go func() {
				var destination tcpstream.Destination
				var err error
				if test.credentials == nil {
					destination, err = ReadConnect(serverConn, time.Second)
				} else {
					destination, err = ReadConnectWithAuth(serverConn, time.Second, test.credentials)
				}
				got := result{destination: destination, err: err}
				if err == nil {
					got.earlyData = make([]byte, len(test.earlyData))
					if deadlineErr := serverConn.SetReadDeadline(time.Now().Add(time.Second)); deadlineErr != nil {
						got.err = deadlineErr
					} else {
						_, got.err = io.ReadFull(serverConn, got.earlyData)
					}
				}
				results <- got
			}()

			if err := writeFull(clientConn, test.greeting); err != nil {
				t.Fatal(err)
			}
			methodReply := make([]byte, 2)
			if _, err := io.ReadFull(clientConn, methodReply); err != nil {
				t.Fatal(err)
			}
			wantMethod := byte(methodNoAuth)
			if test.credentials != nil {
				wantMethod = methodUsernamePassword
			}
			if want := []byte{version5, wantMethod}; !bytes.Equal(methodReply, want) {
				t.Fatalf("method reply = %v, want %v", methodReply, want)
			}

			if test.credentials != nil {
				if err := writeFull(clientConn, test.authRequest); err != nil {
					t.Fatal(err)
				}
				authReply := make([]byte, 2)
				if _, err := io.ReadFull(clientConn, authReply); err != nil {
					t.Fatal(err)
				}
				if want := []byte{authVersion, authSucceeded}; !bytes.Equal(authReply, want) {
					t.Fatalf("authentication reply = %v, want %v", authReply, want)
				}
			}

			requestAndEarlyData := append([]byte(nil), test.request...)
			requestAndEarlyData = append(requestAndEarlyData, test.earlyData...)
			if err := writeFull(clientConn, requestAndEarlyData); err != nil {
				t.Fatal(err)
			}

			select {
			case got := <-results:
				if got.err != nil {
					t.Fatal(got.err)
				}
				if got.destination.String() != test.wantDestination {
					t.Fatalf("destination = %q, want %q", got.destination.String(), test.wantDestination)
				}
				if !bytes.Equal(got.earlyData, test.earlyData) {
					t.Fatalf("early data = %q, want %q", got.earlyData, test.earlyData)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for staged SOCKS5 handshake")
			}
		})
	}
}

func TestReadConnectRejectsInvalidConfiguredCredentials(t *testing.T) {
	credentials := &Credentials{Username: "", Password: "secret"}
	_, err, output, _ := readConnectScript([]byte{version5, 1, methodUsernamePassword}, credentials)
	if !errors.Is(err, ErrAuthenticationFailed) {
		t.Fatalf("ReadConnectWithAuth error = %v, want %v", err, ErrAuthenticationFailed)
	}
	if len(output) != 0 {
		t.Fatalf("output = %v, want none", output)
	}
}

func TestReadConnectRejectsInvalidUsernamePasswordExchange(t *testing.T) {
	credentials := &Credentials{Username: "client", Password: "secret"}
	tests := []struct {
		name       string
		auth       []byte
		wantErr    error
		wantStatus bool
	}{
		{name: "invalid auth version", auth: []byte{2, 1, 'x', 1, 'y'}, wantErr: ErrAuthenticationFailed, wantStatus: true},
		{name: "zero username", auth: []byte{authVersion, 0}, wantErr: ErrAuthenticationFailed, wantStatus: true},
		{name: "short auth header", auth: []byte{authVersion}, wantErr: io.ErrUnexpectedEOF},
		{name: "short username", auth: []byte{authVersion, 3, 'a'}, wantErr: io.ErrUnexpectedEOF},
		{name: "missing password length", auth: []byte{authVersion, 1, 'a'}, wantErr: io.EOF},
		{name: "zero password", auth: []byte{authVersion, 1, 'a', 0}, wantErr: ErrAuthenticationFailed, wantStatus: true},
		{name: "short password", auth: []byte{authVersion, 1, 'a', 3, 'x'}, wantErr: io.ErrUnexpectedEOF},
		{name: "wrong username", auth: []byte{authVersion, 5, 'o', 't', 'h', 'e', 'r', 6, 's', 'e', 'c', 'r', 'e', 't'}, wantErr: ErrAuthenticationFailed, wantStatus: true},
		{name: "wrong password", auth: []byte{authVersion, 6, 'c', 'l', 'i', 'e', 'n', 't', 5, 'w', 'r', 'o', 'n', 'g'}, wantErr: ErrAuthenticationFailed, wantStatus: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err, output, _ := readConnectScript(authInput(test.auth, nil), credentials)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ReadConnectWithAuth error = %v, want %v", err, test.wantErr)
			}
			wantOutput := []byte{version5, methodUsernamePassword}
			if test.wantStatus {
				wantOutput = append(wantOutput, authVersion, authFailed)
			}
			if !bytes.Equal(output, wantOutput) {
				t.Fatalf("output = %v, want %v", output, wantOutput)
			}
		})
	}
}

func TestValidCredentials(t *testing.T) {
	tests := []struct {
		name        string
		credentials Credentials
		want        bool
	}{
		{name: "valid", credentials: Credentials{Username: "u", Password: "p"}, want: true},
		{name: "maximum lengths", credentials: Credentials{Username: strings.Repeat("u", 255), Password: strings.Repeat("p", 255)}, want: true},
		{name: "empty username", credentials: Credentials{Password: "p"}},
		{name: "empty password", credentials: Credentials{Username: "u"}},
		{name: "username too long", credentials: Credentials{Username: strings.Repeat("u", 256), Password: "p"}},
		{name: "password too long", credentials: Credentials{Username: "u", Password: strings.Repeat("p", 256)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validCredentials(test.credentials); got != test.want {
				t.Fatalf("validCredentials() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestConstantTimeEqual(t *testing.T) {
	if got := constantTimeEqual([]byte("same"), []byte("same")); got != 1 {
		t.Fatalf("constantTimeEqual(equal) = %d, want 1", got)
	}
	if got := constantTimeEqual([]byte("same"), []byte("different")); got != 0 {
		t.Fatalf("constantTimeEqual(different) = %d, want 0", got)
	}
}

func TestReadConnectRejectsInvalidRequest(t *testing.T) {
	invalidDomain := connectRequest(addressDomain, domainAddress("bad host"), 443)
	tests := []struct {
		name      string
		request   []byte
		wantErr   error
		wantReply Reply
	}{
		{name: "invalid request version", request: []byte{4, commandConnect, 0, addressIPv4}, wantErr: ErrInvalidRequest, wantReply: ReplyGeneralFailure},
		{name: "nonzero reserved byte", request: []byte{version5, commandConnect, 1, addressIPv4}, wantErr: ErrInvalidRequest, wantReply: ReplyGeneralFailure},
		{name: "BIND command", request: []byte{version5, 2, 0, addressIPv4}, wantErr: ErrCommandNotSupported, wantReply: ReplyCommandNotSupported},
		{name: "UDP ASSOCIATE command", request: []byte{version5, 3, 0, addressIPv4}, wantErr: ErrCommandNotSupported, wantReply: ReplyCommandNotSupported},
		{name: "unknown address type", request: []byte{version5, commandConnect, 0, 0x7f}, wantErr: ErrInvalidRequest, wantReply: ReplyAddressNotSupported},
		{name: "empty domain", request: []byte{version5, commandConnect, 0, addressDomain, 0}, wantErr: ErrInvalidRequest, wantReply: ReplyGeneralFailure},
		{name: "invalid domain", request: invalidDomain, wantErr: ErrInvalidRequest, wantReply: ReplyGeneralFailure},
		{name: "zero port", request: connectRequest(addressIPv4, []byte{192, 0, 2, 1}, 0), wantErr: ErrInvalidRequest, wantReply: ReplyGeneralFailure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			destination, err, output, _ := readConnectScript(noAuthInput(test.request), nil)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ReadConnect error = %v, want %v", err, test.wantErr)
			}
			if !destination.IsZero() {
				t.Fatalf("destination = %q, want zero", destination.String())
			}
			wantOutput := append([]byte{version5, methodNoAuth}, replyPayload(test.wantReply)...)
			if !bytes.Equal(output, wantOutput) {
				t.Fatalf("output = %v, want %v", output, wantOutput)
			}
		})
	}
}

func TestReadConnectDomainBoundaries(t *testing.T) {
	maxLabel := strings.Repeat("a", 63) + ".example"
	overlongLabel := strings.Repeat("a", 64) + ".example"
	maxDomain := strings.Join([]string{
		strings.Repeat("a", 63),
		strings.Repeat("b", 63),
		strings.Repeat("c", 63),
		strings.Repeat("d", 61),
	}, ".")
	overlongDomain := strings.Join([]string{
		strings.Repeat("a", 63),
		strings.Repeat("b", 63),
		strings.Repeat("c", 63),
		strings.Repeat("d", 62),
	}, ".")
	wireMaximumDomain := strings.Repeat("a", 255)
	if len(maxDomain) != 253 || len(overlongDomain) != 254 || len(wireMaximumDomain) != 255 {
		t.Fatalf("test domain lengths = %d, %d, and %d; want 253, 254, and 255", len(maxDomain), len(overlongDomain), len(wireMaximumDomain))
	}

	tests := []struct {
		name    string
		host    string
		wantErr bool
	}{
		{name: "63 byte label", host: maxLabel},
		{name: "64 byte label", host: overlongLabel, wantErr: true},
		{name: "253 byte domain", host: maxDomain},
		{name: "254 byte domain", host: overlongDomain, wantErr: true},
		{name: "255 byte wire maximum", host: wireMaximumDomain, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := connectRequest(addressDomain, domainAddress(test.host), 443)
			destination, err, output, _ := readConnectScript(noAuthInput(request), nil)
			if test.wantErr {
				if !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("ReadConnect error = %v, want %v", err, ErrInvalidRequest)
				}
				if !destination.IsZero() {
					t.Fatalf("destination = %q, want zero", destination.String())
				}
				wantOutput := append([]byte{version5, methodNoAuth}, replyPayload(ReplyGeneralFailure)...)
				if !bytes.Equal(output, wantOutput) {
					t.Fatalf("output = %v, want %v", output, wantOutput)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if destination.Type() != tcpstream.DestinationDomain || destination.Host() != test.host || destination.Port() != 443 {
				t.Fatalf("destination = %q (type %d), want %s:443 (domain)", destination.String(), destination.Type(), test.host)
			}
			if want := []byte{version5, methodNoAuth}; !bytes.Equal(output, want) {
				t.Fatalf("output = %v, want %v", output, want)
			}
		})
	}
}

func TestReadConnectRejectsTruncatedInput(t *testing.T) {
	noAuthGreeting := []byte{version5, 3, 0x80, methodUsernamePassword, methodNoAuth}
	authGreeting := []byte{version5, 2, methodNoAuth, methodUsernamePassword}
	credentials := &Credentials{Username: "client", Password: "secret"}
	authRequest := []byte{authVersion, byte(len(credentials.Username))}
	authRequest = append(authRequest, credentials.Username...)
	authRequest = append(authRequest, byte(len(credentials.Password)))
	authRequest = append(authRequest, credentials.Password...)

	tests := []struct {
		name        string
		greeting    []byte
		auth        []byte
		request     []byte
		credentials *Credentials
	}{
		{
			name:     "no authentication IPv4",
			greeting: noAuthGreeting,
			request:  connectRequest(addressIPv4, []byte{192, 0, 2, 1}, 443),
		},
		{
			name:     "no authentication IPv6",
			greeting: noAuthGreeting,
			request:  connectRequest(addressIPv6, net.ParseIP("2001:db8::1").To16(), 8443),
		},
		{
			name:     "no authentication domain",
			greeting: noAuthGreeting,
			request:  connectRequest(addressDomain, domainAddress("proxy.example.com"), 8388),
		},
		{
			name:        "username password IPv4",
			greeting:    authGreeting,
			auth:        authRequest,
			request:     connectRequest(addressIPv4, []byte{198, 51, 100, 4}, 443),
			credentials: credentials,
		},
		{
			name:        "username password IPv6",
			greeting:    authGreeting,
			auth:        authRequest,
			request:     connectRequest(addressIPv6, net.ParseIP("2001:db8::2").To16(), 8443),
			credentials: credentials,
		},
		{
			name:        "username password domain",
			greeting:    authGreeting,
			auth:        authRequest,
			request:     connectRequest(addressDomain, domainAddress("auth.example.com"), 8388),
			credentials: credentials,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fullInput := append([]byte(nil), test.greeting...)
			fullInput = append(fullInput, test.auth...)
			fullInput = append(fullInput, test.request...)
			for prefixLength := 0; prefixLength < len(fullInput); prefixLength++ {
				t.Run(fmt.Sprintf("prefix_%d", prefixLength), func(t *testing.T) {
					destination, err, output, _ := readConnectScript(fullInput[:prefixLength], test.credentials)
					if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("ReadConnect error = %v, want EOF or unexpected EOF", err)
					}
					if !destination.IsZero() {
						t.Fatalf("destination = %q, want zero", destination.String())
					}

					var wantOutput []byte
					if prefixLength >= len(test.greeting) {
						method := byte(methodNoAuth)
						if test.credentials != nil {
							method = methodUsernamePassword
						}
						wantOutput = []byte{version5, method}
					}
					if test.credentials != nil && prefixLength >= len(test.greeting)+len(test.auth) {
						wantOutput = append(wantOutput, authVersion, authSucceeded)
					}
					if !bytes.Equal(output, wantOutput) {
						t.Fatalf("output = %v, want %v", output, wantOutput)
					}
				})
			}
		})
	}
}

func TestReadConnectPropagatesTimeoutError(t *testing.T) {
	conn := &scriptedConn{reader: errorReader{err: os.ErrDeadlineExceeded}}
	_, err := ReadConnect(conn, time.Second)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("ReadConnect error = %v, want deadline exceeded", err)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("ReadConnect error = %v, want timeout", err)
	}
	if len(conn.deadlineCalls) != 1 || conn.deadlineCalls[0].IsZero() {
		t.Fatalf("deadline calls = %v, want one non-zero deadline", conn.deadlineCalls)
	}
}

func TestReadConnectDeadlineErrors(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		setErr := errors.New("set deadline")
		conn, output := newScriptedConn(nil)
		conn.setDeadlineFunc = func(time.Time) error { return setErr }
		_, err := ReadConnect(conn, time.Second)
		if !errors.Is(err, setErr) {
			t.Fatalf("ReadConnect error = %v, want %v", err, setErr)
		}
		if output.Len() != 0 {
			t.Fatalf("output = %v, want none", output.Bytes())
		}
	})

	t.Run("clear", func(t *testing.T) {
		clearErr := errors.New("clear deadline")
		input := noAuthInput(connectRequest(addressIPv4, []byte{192, 0, 2, 1}, 443))
		conn, output := newScriptedConn(input)
		conn.setDeadlineFunc = func(deadline time.Time) error {
			if deadline.IsZero() {
				return clearErr
			}
			return nil
		}
		destination, err := ReadConnect(conn, time.Second)
		if !errors.Is(err, clearErr) {
			t.Fatalf("ReadConnect error = %v, want %v", err, clearErr)
		}
		if !destination.IsZero() {
			t.Fatalf("destination = %q, want zero on deadline error", destination.String())
		}
		if want := []byte{version5, methodNoAuth}; !bytes.Equal(output.Bytes(), want) {
			t.Fatalf("output = %v, want %v", output.Bytes(), want)
		}
	})
}

func TestReadConnectWriteErrors(t *testing.T) {
	t.Run("method selection", func(t *testing.T) {
		writeErr := errors.New("write method selection")
		conn, _ := newScriptedConn([]byte{version5, 1, methodNoAuth})
		conn.writer = errorWriter{err: writeErr}
		_, err := ReadConnect(conn, time.Second)
		if !errors.Is(err, writeErr) {
			t.Fatalf("ReadConnect error = %v, want %v", err, writeErr)
		}
	})

	t.Run("authentication status", func(t *testing.T) {
		writeErr := errors.New("write authentication status")
		credentials := Credentials{Username: "u", Password: "p"}
		input := authInput([]byte{authVersion, 1, 'u', 1, 'p'}, nil)
		conn, output := newScriptedConn(input)
		conn.writer = &failAfterWriter{writer: output, remaining: 2, err: writeErr}
		_, err := ReadConnectWithAuth(conn, time.Second, &credentials)
		if !errors.Is(err, writeErr) {
			t.Fatalf("ReadConnectWithAuth error = %v, want %v", err, writeErr)
		}
		if want := []byte{version5, methodUsernamePassword}; !bytes.Equal(output.Bytes(), want) {
			t.Fatalf("output = %v, want %v", output.Bytes(), want)
		}
	})
}

func TestReadHostRejectsUnknownAddressType(t *testing.T) {
	_, err := readHost(bytes.NewReader(nil), 0xff)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("readHost error = %v, want %v", err, ErrInvalidRequest)
	}
}

func TestWriteReply(t *testing.T) {
	replies := []struct {
		name  string
		reply Reply
	}{
		{name: "succeeded", reply: ReplySucceeded},
		{name: "general failure", reply: ReplyGeneralFailure},
		{name: "not allowed", reply: ReplyNotAllowed},
		{name: "network unreachable", reply: ReplyNetworkUnreachable},
		{name: "host unreachable", reply: ReplyHostUnreachable},
		{name: "connection refused", reply: ReplyConnectionRefused},
		{name: "TTL expired", reply: ReplyTTLExpired},
		{name: "command not supported", reply: ReplyCommandNotSupported},
		{name: "address not supported", reply: ReplyAddressNotSupported},
	}

	for _, test := range replies {
		t.Run(test.name, func(t *testing.T) {
			output := &bytes.Buffer{}
			conn := &scriptedConn{writer: output}
			if err := WriteReply(conn, test.reply, 0); err != nil {
				t.Fatal(err)
			}
			if want := replyPayload(test.reply); !bytes.Equal(output.Bytes(), want) {
				t.Fatalf("output = %v, want %v", output.Bytes(), want)
			}
			if len(conn.writeDeadlineCalls) != 0 {
				t.Fatalf("write deadline calls = %v, want none", conn.writeDeadlineCalls)
			}
		})
	}
}

func TestWriteReplyNegativeTimeoutPreservesWriteDeadline(t *testing.T) {
	output := &bytes.Buffer{}
	conn := &scriptedConn{writer: output}
	if err := WriteReply(conn, ReplySucceeded, -time.Second); err != nil {
		t.Fatal(err)
	}
	if want := replyPayload(ReplySucceeded); !bytes.Equal(output.Bytes(), want) {
		t.Fatalf("output = %v, want %v", output.Bytes(), want)
	}
	if len(conn.writeDeadlineCalls) != 0 {
		t.Fatalf("write deadline calls = %v, want none", conn.writeDeadlineCalls)
	}
}

func TestWriteReplyHandlesPartialWrites(t *testing.T) {
	output := &bytes.Buffer{}
	conn := &scriptedConn{writer: &chunkWriter{writer: output, max: 2}}
	if err := WriteReply(conn, ReplySucceeded, time.Second); err != nil {
		t.Fatal(err)
	}
	if want := replyPayload(ReplySucceeded); !bytes.Equal(output.Bytes(), want) {
		t.Fatalf("output = %v, want %v", output.Bytes(), want)
	}
	assertDeadlineCleared(t, conn.writeDeadlineCalls)
}

func TestWriteReplyErrors(t *testing.T) {
	t.Run("invalid reply", func(t *testing.T) {
		output := &bytes.Buffer{}
		conn := &scriptedConn{writer: output}
		err := WriteReply(conn, ReplyAddressNotSupported+1, time.Second)
		if !errors.Is(err, ErrInvalidReply) {
			t.Fatalf("WriteReply error = %v, want %v", err, ErrInvalidReply)
		}
		if output.Len() != 0 {
			t.Fatalf("output = %v, want none", output.Bytes())
		}
		if len(conn.writeDeadlineCalls) != 0 {
			t.Fatalf("write deadline calls = %v, want none", conn.writeDeadlineCalls)
		}
	})

	t.Run("set deadline", func(t *testing.T) {
		setErr := errors.New("set write deadline")
		output := &bytes.Buffer{}
		conn := &scriptedConn{
			writer: output,
			setWriteDeadlineFn: func(deadline time.Time) error {
				if !deadline.IsZero() {
					return setErr
				}
				return nil
			},
		}
		if err := WriteReply(conn, ReplySucceeded, time.Second); !errors.Is(err, setErr) {
			t.Fatalf("WriteReply error = %v, want %v", err, setErr)
		}
		if output.Len() != 0 {
			t.Fatalf("output = %v, want none", output.Bytes())
		}
		if len(conn.writeDeadlineCalls) != 1 {
			t.Fatalf("write deadline calls = %v, want one", conn.writeDeadlineCalls)
		}
	})

	t.Run("write", func(t *testing.T) {
		writeErr := errors.New("write reply")
		conn := &scriptedConn{writer: errorWriter{err: writeErr}}
		if err := WriteReply(conn, ReplySucceeded, time.Second); !errors.Is(err, writeErr) {
			t.Fatalf("WriteReply error = %v, want %v", err, writeErr)
		}
		assertDeadlineCleared(t, conn.writeDeadlineCalls)
	})

	t.Run("zero byte write", func(t *testing.T) {
		conn := &scriptedConn{writer: zeroWriter{}}
		if err := WriteReply(conn, ReplySucceeded, time.Second); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("WriteReply error = %v, want %v", err, io.ErrShortWrite)
		}
		assertDeadlineCleared(t, conn.writeDeadlineCalls)
	})

	t.Run("clear deadline", func(t *testing.T) {
		clearErr := errors.New("clear write deadline")
		output := &bytes.Buffer{}
		conn := &scriptedConn{
			writer: output,
			setWriteDeadlineFn: func(deadline time.Time) error {
				if deadline.IsZero() {
					return clearErr
				}
				return nil
			},
		}
		if err := WriteReply(conn, ReplySucceeded, time.Second); !errors.Is(err, clearErr) {
			t.Fatalf("WriteReply error = %v, want %v", err, clearErr)
		}
		if want := replyPayload(ReplySucceeded); !bytes.Equal(output.Bytes(), want) {
			t.Fatalf("output = %v, want %v", output.Bytes(), want)
		}
		assertDeadlineCleared(t, conn.writeDeadlineCalls)
	})

	t.Run("write error takes precedence over clear error", func(t *testing.T) {
		writeErr := errors.New("write reply")
		clearErr := errors.New("clear write deadline")
		conn := &scriptedConn{
			writer: errorWriter{err: writeErr},
			setWriteDeadlineFn: func(deadline time.Time) error {
				if deadline.IsZero() {
					return clearErr
				}
				return nil
			},
		}
		if err := WriteReply(conn, ReplySucceeded, time.Second); !errors.Is(err, writeErr) {
			t.Fatalf("WriteReply error = %v, want %v", err, writeErr)
		}
		assertDeadlineCleared(t, conn.writeDeadlineCalls)
	})
}

func TestWriteReplyPropagatesTimeoutError(t *testing.T) {
	conn := &scriptedConn{writer: errorWriter{err: os.ErrDeadlineExceeded}}
	err := WriteReply(conn, ReplySucceeded, time.Second)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("WriteReply error = %v, want deadline exceeded", err)
	}
	assertDeadlineCleared(t, conn.writeDeadlineCalls)
}

func TestReplyForError(t *testing.T) {
	var typedNil *tcpstream.OpenError
	tests := []struct {
		name string
		err  error
		want Reply
	}{
		{name: "nil", want: ReplyGeneralFailure},
		{name: "typed nil", err: typedNil, want: ReplyGeneralFailure},
		{name: "generic", err: errors.New("open failed"), want: ReplyGeneralFailure},
		{name: "success result", err: &tcpstream.OpenError{Result: tcpstream.OpenResultSuccess}, want: ReplyGeneralFailure},
		{name: "general failure result", err: &tcpstream.OpenError{Result: tcpstream.OpenResultGeneralFailure}, want: ReplyGeneralFailure},
		{name: "connection refused", err: &tcpstream.OpenError{Result: tcpstream.OpenResultConnectionRefused}, want: ReplyConnectionRefused},
		{name: "network unreachable", err: &tcpstream.OpenError{Result: tcpstream.OpenResultNetworkUnreachable}, want: ReplyNetworkUnreachable},
		{name: "host unreachable", err: &tcpstream.OpenError{Result: tcpstream.OpenResultHostUnreachable}, want: ReplyHostUnreachable},
		{name: "timeout", err: &tcpstream.OpenError{Result: tcpstream.OpenResultTimeout}, want: ReplyTTLExpired},
		{name: "policy denied", err: &tcpstream.OpenError{Result: tcpstream.OpenResultPolicyDenied}, want: ReplyNotAllowed},
		{name: "unknown result", err: &tcpstream.OpenError{Result: tcpstream.OpenResult(0xff)}, want: ReplyGeneralFailure},
		{
			name: "wrapped open error",
			err:  fmt.Errorf("wrapped: %w", &tcpstream.OpenError{Result: tcpstream.OpenResultConnectionRefused}),
			want: ReplyConnectionRefused,
		},
		{
			name: "network operation wrapping open error",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: fmt.Errorf("destination: %w", &tcpstream.OpenError{Result: tcpstream.OpenResultHostUnreachable}),
			},
			want: ReplyHostUnreachable,
		},
		{
			name: "joined wrapped open error",
			err: errors.Join(
				errors.New("carrier failed"),
				fmt.Errorf("destination: %w", &tcpstream.OpenError{Result: tcpstream.OpenResultTimeout}),
			),
			want: ReplyTTLExpired,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ReplyForError(test.err); got != test.want {
				t.Fatalf("ReplyForError(%v) = %d, want %d", test.err, got, test.want)
			}
		})
	}
}
