//go:build !linux

package udp

import "net"

func OpenUDPOnInterface(laddr *net.UDPAddr, ifName string) (*net.UDPConn, error) {
	return net.ListenUDP("udp", laddr)
}
