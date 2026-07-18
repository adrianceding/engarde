//go:build !linux

package tcpbind

import "net"

func setInterface(_ *net.Dialer, _ string) {}
