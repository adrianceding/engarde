package socks5

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/adrianceding/engarde/internal/tcpstream"
)

const (
	version5               = 0x05
	methodNoAuth           = 0x00
	methodUsernamePassword = 0x02
	methodUnavailable      = 0xff
	authVersion            = 0x01
	authSucceeded          = 0x00
	authFailed             = 0x01
	commandConnect         = 0x01
	addressIPv4            = 0x01
	addressDomain          = 0x03
	addressIPv6            = 0x04
)

type Credentials struct {
	Username string
	Password string
}

type Reply uint8

const (
	ReplySucceeded Reply = iota
	ReplyGeneralFailure
	ReplyNotAllowed
	ReplyNetworkUnreachable
	ReplyHostUnreachable
	ReplyConnectionRefused
	ReplyTTLExpired
	ReplyCommandNotSupported
	ReplyAddressNotSupported
)

var (
	ErrInvalidRequest       = errors.New("invalid SOCKS5 request")
	ErrInvalidReply         = errors.New("invalid SOCKS5 reply")
	ErrNoAcceptableMethod   = errors.New("SOCKS5 client does not support the required authentication method")
	ErrAuthenticationFailed = errors.New("SOCKS5 authentication failed")
	ErrCommandNotSupported  = errors.New("SOCKS5 command is not supported")
)

// ReadConnect reads a SOCKS5 CONNECT handshake. A non-positive timeout leaves
// the connection's existing deadlines unchanged.
func ReadConnect(conn net.Conn, timeout time.Duration) (tcpstream.Destination, error) {
	return ReadConnectWithAuth(conn, timeout, nil)
}

// ReadConnectWithAuth reads a SOCKS5 CONNECT handshake and optionally requires
// username/password authentication. A non-positive timeout leaves the
// connection's existing deadlines unchanged.
func ReadConnectWithAuth(conn net.Conn, timeout time.Duration, credentials *Credentials) (tcpstream.Destination, error) {
	deadlineSet := timeout > 0
	if deadlineSet {
		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			return tcpstream.Destination{}, err
		}
	}

	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil {
		return tcpstream.Destination{}, err
	}
	if greeting[0] != version5 || greeting[1] == 0 {
		return tcpstream.Destination{}, ErrInvalidRequest
	}
	methods := make([]byte, int(greeting[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return tcpstream.Destination{}, err
	}
	requiredMethod := byte(methodNoAuth)
	if credentials != nil {
		if !validCredentials(*credentials) {
			return tcpstream.Destination{}, ErrAuthenticationFailed
		}
		requiredMethod = methodUsernamePassword
	}
	accepted := false
	for _, method := range methods {
		if method == requiredMethod {
			accepted = true
			break
		}
	}
	if !accepted {
		_ = writeFull(conn, []byte{version5, methodUnavailable})
		return tcpstream.Destination{}, ErrNoAcceptableMethod
	}
	if err := writeFull(conn, []byte{version5, requiredMethod}); err != nil {
		return tcpstream.Destination{}, err
	}
	if credentials != nil {
		if err := authenticate(conn, *credentials); err != nil {
			return tcpstream.Destination{}, err
		}
	}

	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return tcpstream.Destination{}, err
	}
	if header[0] != version5 || header[2] != 0 {
		_ = WriteReply(conn, ReplyGeneralFailure, 0)
		return tcpstream.Destination{}, ErrInvalidRequest
	}
	if header[1] != commandConnect {
		_ = WriteReply(conn, ReplyCommandNotSupported, 0)
		return tcpstream.Destination{}, ErrCommandNotSupported
	}

	if header[3] != addressIPv4 && header[3] != addressIPv6 && header[3] != addressDomain {
		_ = WriteReply(conn, ReplyAddressNotSupported, 0)
		return tcpstream.Destination{}, ErrInvalidRequest
	}
	host, err := readHost(conn, header[3])
	if err != nil {
		if errors.Is(err, ErrInvalidRequest) {
			_ = WriteReply(conn, ReplyGeneralFailure, 0)
		}
		return tcpstream.Destination{}, err
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return tcpstream.Destination{}, err
	}
	port := uint16(portBytes[0])<<8 | uint16(portBytes[1])
	destination, err := tcpstream.NewDestination(host, port)
	if err != nil {
		_ = WriteReply(conn, ReplyGeneralFailure, 0)
		return tcpstream.Destination{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if deadlineSet {
		if err := conn.SetDeadline(time.Time{}); err != nil {
			return tcpstream.Destination{}, err
		}
	}
	return destination, nil
}

func authenticate(conn net.Conn, credentials Credentials) error {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}
	if header[0] != authVersion || header[1] == 0 {
		_ = writeFull(conn, []byte{authVersion, authFailed})
		return ErrAuthenticationFailed
	}
	username := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, username); err != nil {
		return err
	}
	passwordLength := []byte{0}
	if _, err := io.ReadFull(conn, passwordLength); err != nil {
		return err
	}
	if passwordLength[0] == 0 {
		_ = writeFull(conn, []byte{authVersion, authFailed})
		return ErrAuthenticationFailed
	}
	password := make([]byte, int(passwordLength[0]))
	if _, err := io.ReadFull(conn, password); err != nil {
		return err
	}
	usernameMatch := constantTimeEqual(username, []byte(credentials.Username))
	passwordMatch := constantTimeEqual(password, []byte(credentials.Password))
	if usernameMatch&passwordMatch != 1 {
		_ = writeFull(conn, []byte{authVersion, authFailed})
		return ErrAuthenticationFailed
	}
	if err := writeFull(conn, []byte{authVersion, authSucceeded}); err != nil {
		return err
	}
	return nil
}

func validCredentials(credentials Credentials) bool {
	usernameLength := len([]byte(credentials.Username))
	passwordLength := len([]byte(credentials.Password))
	return usernameLength > 0 && usernameLength <= 255 && passwordLength > 0 && passwordLength <= 255
}

func constantTimeEqual(actual, expected []byte) int {
	actualHash := sha256.Sum256(actual)
	expectedHash := sha256.Sum256(expected)
	return subtle.ConstantTimeCompare(actualHash[:], expectedHash[:])
}

// WriteReply writes a SOCKS5 CONNECT reply. A non-positive timeout leaves the
// connection's existing write deadline unchanged.
func WriteReply(conn net.Conn, reply Reply, timeout time.Duration) (err error) {
	if reply > ReplyAddressNotSupported {
		return fmt.Errorf("%w: %d", ErrInvalidReply, reply)
	}
	deadlineSet := timeout > 0
	if deadlineSet {
		if err = conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		defer func() {
			if clearErr := conn.SetWriteDeadline(time.Time{}); err == nil {
				err = clearErr
			}
		}()
	}
	if err = writeFull(conn, []byte{version5, byte(reply), 0, addressIPv4, 0, 0, 0, 0, 0, 0}); err != nil {
		return err
	}
	return nil
}

func ReplyForError(err error) Reply {
	var openError *tcpstream.OpenError
	if !errors.As(err, &openError) || openError == nil {
		return ReplyGeneralFailure
	}
	switch openError.Result {
	case tcpstream.OpenResultConnectionRefused:
		return ReplyConnectionRefused
	case tcpstream.OpenResultNetworkUnreachable:
		return ReplyNetworkUnreachable
	case tcpstream.OpenResultHostUnreachable:
		return ReplyHostUnreachable
	case tcpstream.OpenResultTimeout:
		return ReplyTTLExpired
	case tcpstream.OpenResultPolicyDenied:
		return ReplyNotAllowed
	default:
		return ReplyGeneralFailure
	}
}

func readHost(reader io.Reader, addressType byte) (string, error) {
	switch addressType {
	case addressIPv4:
		value := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(reader, value); err != nil {
			return "", err
		}
		return net.IP(value).String(), nil
	case addressIPv6:
		value := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(reader, value); err != nil {
			return "", err
		}
		return net.IP(value).String(), nil
	case addressDomain:
		length := []byte{0}
		if _, err := io.ReadFull(reader, length); err != nil {
			return "", err
		}
		if length[0] == 0 {
			return "", ErrInvalidRequest
		}
		value := make([]byte, int(length[0]))
		if _, err := io.ReadFull(reader, value); err != nil {
			return "", err
		}
		return string(value), nil
	default:
		return "", ErrInvalidRequest
	}
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}
