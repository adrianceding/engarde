package tcpbind

import (
	"context"
	"net"
	"time"
)

func DialContext(ctx context.Context, destination, sourceIP, interfaceName string, timeout time.Duration) (net.Conn, error) {
	localAddress := &net.TCPAddr{IP: net.ParseIP(sourceIP)}
	dialer := net.Dialer{
		Timeout:   timeout,
		LocalAddr: localAddress,
		KeepAlive: 30 * time.Second,
	}
	setInterface(&dialer, interfaceName)
	conn, err := dialer.DialContext(ctx, "tcp4", destination)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}
	return conn, nil
}
