//go:build windows

package serverrole

import (
	"errors"

	"github.com/adrianceding/engarde/internal/tcpstream"
	"golang.org/x/sys/windows"
)

func platformOpenResult(err error) tcpstream.OpenResult {
	switch {
	case errors.Is(err, windows.WSAECONNREFUSED):
		return tcpstream.OpenResultConnectionRefused
	case errors.Is(err, windows.WSAENETUNREACH):
		return tcpstream.OpenResultNetworkUnreachable
	case errors.Is(err, windows.WSAEHOSTUNREACH):
		return tcpstream.OpenResultHostUnreachable
	default:
		return tcpstream.OpenResultGeneralFailure
	}
}
