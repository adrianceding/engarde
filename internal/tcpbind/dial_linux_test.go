//go:build linux

package tcpbind

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSetInterfaceLinuxEmptyNameLeavesControlUnset(t *testing.T) {
	dialer := &net.Dialer{}
	setInterface(dialer, "")
	if dialer.Control != nil {
		t.Fatal("empty interface installed a socket control callback")
	}
}

func TestSetInterfaceLinuxPropagatesRawControlError(t *testing.T) {
	want := errors.New("raw control")
	dialer := &net.Dialer{}
	setInterface(dialer, "engarde-test")
	if dialer.Control == nil {
		t.Fatal("non-empty interface did not install a socket control callback")
	}
	err := dialer.Control("tcp4", "127.0.0.1:1", failingTCPBindRawConn{err: want})
	if !errors.Is(err, want) {
		t.Fatalf("socket control error = %v, want %v", err, want)
	}
}

func TestDialContextLinuxRejectsMissingInterface(t *testing.T) {
	const interfaceName = "engarde-miss"
	if _, err := net.InterfaceByName(interfaceName); err == nil {
		t.Fatalf("test interface %q unexpectedly exists", interfaceName)
	}
	listener := newTCPBindTestListener(t)
	ctx, cancel := context.WithTimeout(context.Background(), tcpbindTestTimeout)
	defer cancel()
	conn, err := DialContext(ctx, listener.Addr().String(), "127.0.0.1", interfaceName, time.Second)
	if conn != nil {
		_ = conn.Close()
		t.Fatalf("DialContext unexpectedly bound missing interface %q", interfaceName)
	}
	if err == nil {
		t.Fatalf("DialContext accepted missing interface %q", interfaceName)
	}
}

func TestSetInterfaceLinuxBindsSocketWhenPermitted(t *testing.T) {
	listener := newTCPBindTestListener(t)
	rawConn, err := listener.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	dialer := &net.Dialer{}
	setInterface(dialer, "lo")
	if err := dialer.Control("tcp4", listener.Addr().String(), rawConn); errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
		t.Skipf("SO_BINDTODEVICE requires unavailable privilege: %v", err)
	} else if err != nil {
		t.Fatal(err)
	}
	var boundInterface string
	var socketErr error
	if err := rawConn.Control(func(fileDescriptor uintptr) {
		boundInterface, socketErr = unix.GetsockoptString(int(fileDescriptor), unix.SOL_SOCKET, unix.SO_BINDTODEVICE)
	}); err != nil {
		t.Fatal(err)
	}
	if socketErr != nil {
		t.Fatal(socketErr)
	}
	if boundInterface != "lo" {
		t.Fatalf("SO_BINDTODEVICE = %q, want lo", boundInterface)
	}
}

type failingTCPBindRawConn struct {
	err error
}

func (conn failingTCPBindRawConn) Control(func(uintptr)) error {
	return conn.err
}

func (conn failingTCPBindRawConn) Read(func(uintptr) bool) error {
	return conn.err
}

func (conn failingTCPBindRawConn) Write(func(uintptr) bool) error {
	return conn.err
}

var _ syscall.RawConn = failingTCPBindRawConn{}
