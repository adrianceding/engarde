//go:build !windows

package serverrole

import "syscall"

var (
	connectionRefusedError  = syscall.ECONNREFUSED
	networkUnreachableError = syscall.ENETUNREACH
	hostUnreachableError    = syscall.EHOSTUNREACH
)
