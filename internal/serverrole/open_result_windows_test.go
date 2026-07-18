//go:build windows

package serverrole

import "golang.org/x/sys/windows"

var (
	connectionRefusedError  = windows.WSAECONNREFUSED
	networkUnreachableError = windows.WSAENETUNREACH
	hostUnreachableError    = windows.WSAEHOSTUNREACH
)
