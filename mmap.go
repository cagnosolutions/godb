package godb

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

const (
	PROT  uint = syscall.PROT_READ | syscall.PROT_WRITE
	FLAGS uint = syscall.MAP_SHARED
)

type mmap []byte

func mmap_at(addr uintptr, fd uintptr, offset, length int64, prot uint, flags uint) (mmap, error) {
	if length == -1 {
		var stat syscall.Stat_t
		if err := syscall.Fstat(int(fd), &stat); err != nil {
			return nil, err
		}
		length = stat.Size
	}
	addr, err := mmap_syscall(addr, uintptr(length), uintptr(prot), uintptr(flags), fd, offset)
	if err != syscall.Errno(0) {
		return nil, err
	}
	mm := mmap{}
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&mm))
	dh.Data = addr
	dh.Len = int(length) // umm... truncating here feels like trouble??
	dh.Cap = dh.Len
	return mm, nil
}

func (mm mmap) Munmap() {
	dh := *(*reflect.SliceHeader)(unsafe.Pointer(&mm))
	_, _, err := syscall.Syscall(syscall.SYS_MUNMAP, uintptr(dh.Data), uintptr(dh.Len), 0)
	if err != 0 {
		panic(err)
	}
}

func (mm mmap) Sync() {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mm))
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, uintptr(rh.Data), uintptr(rh.Len), uintptr(syscall.MS_ASYNC))
	if err != 0 {
		panic(err)
	}
}

func (mm mmap) IsResident() ([]bool, error) {
	sz := os.Getpagesize()                             // page size
	re := make([]bool, (len(mm)+sz-1)/sz)              // result
	dh := *(*reflect.SliceHeader)(unsafe.Pointer(&mm)) // result data ptr
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&re)) // result data header ptr
	_, _, err := syscall.Syscall(syscall.SYS_MINCORE, uintptr(dh.Data), uintptr(dh.Len), uintptr(rh.Data))
	for i := range re {
		*(*uint8)(unsafe.Pointer(&re[i])) &= 1
	}
	if err != 0 {
		return nil, err
	}
	return re, nil
}

func MapFile(fd *os.File) mmap {
	fi := GetFileInfo(fd)
	mm, err := mmap_at(0, fd.Fd(), 0, fi.Size(), PROT, FLAGS)
	if err != nil {
		panic(err)
	}
	return mm
}
