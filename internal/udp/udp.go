package udp

import "net"

func OpenUDP(laddr *net.UDPAddr) (*net.UDPConn, error) {
	return OpenUDPOnInterface(laddr, "")
}
