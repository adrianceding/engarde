package udp

import (
	"net"
	"testing"
)

func TestOpenUDP(t *testing.T) {
	conn, err := OpenUDP(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("OpenUDP returned error: %v", err)
	}
	defer conn.Close()
	if conn.LocalAddr() == nil {
		t.Fatal("OpenUDP returned connection without local address")
	}
}
