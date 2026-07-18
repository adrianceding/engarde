//go:build linux

package tcpbind

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

func setInterface(dialer *net.Dialer, interfaceName string) {
	if interfaceName == "" {
		return
	}
	dialer.Control = func(_, _ string, rawConn syscall.RawConn) error {
		var setErr error
		if err := rawConn.Control(func(fileDescriptor uintptr) {
			setErr = unix.SetsockoptString(int(fileDescriptor), unix.SOL_SOCKET, unix.SO_BINDTODEVICE, interfaceName)
		}); err != nil {
			return err
		}
		return setErr
	}
}
