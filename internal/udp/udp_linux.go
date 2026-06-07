//go:build linux

package udp

import (
	"fmt"
	"net"
	"os"
	"syscall"
)

var sysSocket = syscall.Socket
var sysSetsockoptInt = syscall.SetsockoptInt
var sysSetsockoptString = syscall.SetsockoptString
var sysBind = syscall.Bind
var sysClose = syscall.Close
var newFile = os.NewFile
var filePacketConn = net.FilePacketConn

func OpenUDPOnInterface(laddr *net.UDPAddr, ifName string) (*net.UDPConn, error) {
	if laddr == nil {
		laddr = &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	}

	socket, err := sysSocket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err != nil {
		return nil, fmt.Errorf("create udp socket laddr=%v ifname=%s: %w", laddr, ifName, err)
	}
	if err := sysSetsockoptInt(socket, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		sysClose(socket)
		return nil, fmt.Errorf("set reuse addr laddr=%v ifname=%s: %w", laddr, ifName, err)
	}
	if ifName != "" {
		if err := sysSetsockoptString(socket, syscall.SOL_SOCKET, syscall.SO_BINDTODEVICE, ifName); err != nil {
			sysClose(socket)
			return nil, fmt.Errorf("bind udp socket to device laddr=%v ifname=%s: %w", laddr, ifName, err)
		}
	}

	sockaddr := syscall.SockaddrInet4{Port: laddr.Port}
	copy(sockaddr.Addr[:], laddr.IP.To4())
	if err := sysBind(socket, &sockaddr); err != nil {
		sysClose(socket)
		return nil, fmt.Errorf("bind udp socket laddr=%v ifname=%s: %w", laddr, ifName, err)
	}

	file := newFile(uintptr(socket), "")
	conn, err := filePacketConn(file)
	file.Close()
	if err != nil {
		sysClose(socket)
		return nil, fmt.Errorf("convert udp socket laddr=%v ifname=%s: %w", laddr, ifName, err)
	}

	udpConn, ok := conn.(*net.UDPConn)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("converted packet connection is %T, not *net.UDPConn", conn)
	}
	return udpConn, nil
}
