package ngin

import (
	"bytes"
	"os"
	"syscall"
	"unsafe"
)

type mmap []byte

func Mmap(f *os.File, off, len int) mmap {
	mm, err := syscall.Mmap(int(f.Fd()), int64(off), len, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	return mm
}

func (mm mmap) Mlock() {
	err := syscall.Mlock(mm)
	if err != nil {
		panic(err)
	}
}

func (mm mmap) Munlock() {
	err := syscall.Munlock(mm)
	if err != nil {
		panic(err)
	}
}

func (mm mmap) Munmap() {
	err := syscall.Munmap(mm)
	mm = nil
	if err != nil {
		panic(err)
	}
}

func (mm mmap) Sync() {
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&mm[0])), uintptr(len(mm)),
		uintptr(syscall.MS_ASYNC))
	if err != 0 {
		panic(err)
	}
}

func (mm mmap) Mremap(size int) mmap {
	fd := uintptr(unsafe.Pointer(&mm[0]))
	err := syscall.Munmap(mm)
	mm = nil
	if err != nil {
		panic(err)
	}
	err = syscall.Ftruncate(int(fd), int64(align(size)))
	if err != nil {
		panic(err)
	}
	mm, err = syscall.Mmap(int(fd), int64(0), size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	return mm
}

/*
func Open(path string) (*os.File, string, int) {
	fd, err := os.OpenFile(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	fi, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	return fd, sanitize(fi.Name()), int(fi.Size())
}

func sanitize(path string) string {
	if path[len(path)-1] == '/' {
		return path[:len(path)-1]
	}
	if x := strings.Index(path, "."); x != -1 {
		return path[:x]
	}
	return path
}
*/

func align(size int) int {
	if size > 0 {
		return (size + page - 1) &^ (page - 1)
	}
	return page
}

func resize(fd uintptr, size int) int {
	err := syscall.Ftruncate(int(fd), int64(align(size)))
	if err != nil {
		panic(err)
	}
	return size
}

func (mm mmap) Len() int {
	return len(mm) / page
}

func (mm mmap) Less(i, j int) bool {
	pi, pj := i*page, j*page
	if mm[pi] == 0x00 {
		if mm[pi] == mm[pj] {
			return true
		}
		return false
	}
	if mm[pj] == 0x00 {
		return true
	}
	return bytes.Compare(mm[pi:pi+page], mm[pj:pj+page]) == -1

}

func (mm mmap) Swap(i, j int) {
	pi, pj := i*page, j*page
	copy(temp, mm[pi:pi+page])
	copy(mm[pi:pi+page], mm[pj:pj+page])
	copy(mm[pj:pj+page], temp)
}
