//go:build linux

package udp

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

type fakePacketConn struct{}

func (fakePacketConn) ReadFrom([]byte) (int, net.Addr, error) { return 0, nil, io.EOF }
func (fakePacketConn) WriteTo([]byte, net.Addr) (int, error)  { return 0, io.ErrClosedPipe }
func (fakePacketConn) Close() error                           { return nil }
func (fakePacketConn) LocalAddr() net.Addr                    { return &net.UDPAddr{} }
func (fakePacketConn) SetDeadline(time.Time) error            { return nil }
func (fakePacketConn) SetReadDeadline(time.Time) error        { return nil }
func (fakePacketConn) SetWriteDeadline(time.Time) error       { return nil }

func resetLinuxHooks(t *testing.T) {
	t.Helper()
	originalSocket := sysSocket
	originalSetsockoptInt := sysSetsockoptInt
	originalSetsockoptString := sysSetsockoptString
	originalBind := sysBind
	originalClose := sysClose
	originalNewFile := newFile
	originalFilePacketConn := filePacketConn
	t.Cleanup(func() {
		sysSocket = originalSocket
		sysSetsockoptInt = originalSetsockoptInt
		sysSetsockoptString = originalSetsockoptString
		sysBind = originalBind
		sysClose = originalClose
		newFile = originalNewFile
		filePacketConn = originalFilePacketConn
	})
}

func TestOpenUDPOnInterfaceLoopback(t *testing.T) {
	conn, err := OpenUDPOnInterface(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}, "")
	if err != nil {
		t.Fatalf("OpenUDPOnInterface returned error: %v", err)
	}
	defer conn.Close()
	if conn.LocalAddr() == nil {
		t.Fatal("connection has no local address")
	}
}

func TestOpenUDPOnInterfaceNilAddr(t *testing.T) {
	conn, err := OpenUDPOnInterface(nil, "")
	if err != nil {
		t.Fatalf("OpenUDPOnInterface returned error: %v", err)
	}
	defer conn.Close()
}

func TestOpenUDPOnInterfaceErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(error)
		want  string
	}{
		{name: "socket", setup: func(err error) { sysSocket = func(domain, typ, proto int) (int, error) { return -1, err } }, want: "create udp socket"},
		{name: "reuse", setup: func(err error) { sysSetsockoptInt = func(fd, level, opt, value int) error { return err } }, want: "set reuse addr"},
		{name: "device", setup: func(err error) { sysSetsockoptString = func(fd, level, opt int, value string) error { return err } }, want: "bind udp socket to device"},
		{name: "bind", setup: func(err error) { sysBind = func(fd int, sa syscall.Sockaddr) error { return err } }, want: "bind udp socket"},
		{name: "convert", setup: func(err error) { filePacketConn = func(file *os.File) (net.PacketConn, error) { return nil, err } }, want: "convert udp socket"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetLinuxHooks(t)
			testErr := errors.New(test.name)
			test.setup(testErr)
			_, err := OpenUDPOnInterface(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}, "eth0")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestOpenUDPOnInterfaceRejectsNonUDPConn(t *testing.T) {
	resetLinuxHooks(t)
	filePacketConn = func(file *os.File) (net.PacketConn, error) { return fakePacketConn{}, nil }
	_, err := OpenUDPOnInterface(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}, "")
	if err == nil || !strings.Contains(err.Error(), "not *net.UDPConn") {
		t.Fatalf("error = %v, want non-UDP conversion error", err)
	}
}
