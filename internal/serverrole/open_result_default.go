//go:build !windows

package serverrole

import (
	"errors"
	"syscall"

	"github.com/adrianceding/engarde/internal/tcpstream"
)

func platformOpenResult(err error) tcpstream.OpenResult {
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return tcpstream.OpenResultConnectionRefused
	case errors.Is(err, syscall.ENETUNREACH):
		return tcpstream.OpenResultNetworkUnreachable
	case errors.Is(err, syscall.EHOSTUNREACH):
		return tcpstream.OpenResultHostUnreachable
	default:
		return tcpstream.OpenResultGeneralFailure
	}
}
