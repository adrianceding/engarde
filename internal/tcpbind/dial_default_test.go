//go:build !linux

package tcpbind

import (
	"net"
	"syscall"
	"testing"
)

func TestSetInterfaceFallbackLeavesControlUnchanged(t *testing.T) {
	called := false
	dialer := &net.Dialer{Control: func(string, string, syscall.RawConn) error {
		called = true
		return nil
	}}
	setInterface(dialer, "ignored-on-this-platform")
	if dialer.Control == nil {
		t.Fatal("fallback removed the existing socket control callback")
	}
	if err := dialer.Control("tcp4", "127.0.0.1:1", nil); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("fallback replaced the existing socket control callback")
	}
}
