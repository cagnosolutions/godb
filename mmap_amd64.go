// +build darwin dragonfly linux freebsd openbsd netbsd solaris
// +build amd64

package godb

import "syscall"

// NOTE: NEW unix_amd64
func mmap_syscall(addr, length, prot, flags, fd uintptr, offset int64) (uintptr, error) {
	addr, _, err := syscall.Syscall6(syscall.SYS_MMAP, addr, length, prot, flags, fd, uintptr(offset))
	return addr, err
}
