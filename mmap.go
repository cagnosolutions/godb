package godb

import (
	"fmt"
	"os"
	"reflect"
	"syscall"
	"time"
	"unsafe"
)

const (
	PROT  uint = syscall.PROT_READ | syscall.PROT_WRITE
	FLAGS uint = syscall.MAP_SHARED
)

type mmap []byte

// NOTE: UPDATED
func Mmap(fd *os.File, off, len int) mmap {
	//mm, err := syscall.Mmap(int(f.Fd()), int64(off), len, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	mm, err := mmap_at(0, fd.Fd(), int64(off), int64(len), PROT, FLAGS)
	if err != nil {
		panic(err)
	}
	return mm
}

// NOTE: NEW
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

// NOTE: NEW
func (mm mmap) Munmap() {
	dh := *(*reflect.SliceHeader)(unsafe.Pointer(&mm))
	t1 := time.Now().UnixNano()
	_, _, err := syscall.Syscall(syscall.SYS_MUNMAP, uintptr(dh.Data), uintptr(dh.Len), 0)
	t2 := time.Now().UnixNano()
	fmt.Printf("syscall.Munmap(mm):\n\tnanoseconds: %d\n\tmicroseconds: %d\n\tmilliseconds: %d\n\n", t2-t1, (t2-t1)/1000, ((t2-t1)/1000)/1000)
	if err != 0 {
		panic(err)
	}
}

// NOTE: NEW
func (mm mmap) Sync() {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mm))
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, uintptr(rh.Data), uintptr(rh.Len), uintptr(syscall.MS_ASYNC))
	if err != 0 {
		panic(err)
	}
}

// NOTE: NEW
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

/*// NOTE: NOT USED
func (mm mmap) _Mlock() {
	err := syscall.Mlock(mm)
	if err != nil {
		panic(err)
	}
}*/

/*// NOTE: NOT USED
func (mm mmap) _Munlock() {
	err := syscall.Munlock(mm)
	if err != nil {
		panic(err)
	}
}*/

/*// NOTE: DEPRECATED
func (mm mmap) _Munmap() {
	t1 := time.Now().UnixNano()
	err := syscall.Munmap(mm)
	t2 := time.Now().UnixNano()
	fmt.Printf("syscall.Munmap(mm):\n\tnanoseconds: %d\n\tmicroseconds: %d\n\tmilliseconds: %d\n\n", t2-t1, (t2-t1)/1000, ((t2-t1)/1000)/1000)
	mm = nil
	if err != nil {
		panic(err)
	}
}*/

/*// NOTE: DEPRECATED
func (mm mmap) _Sync() {
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&mm[0])), uintptr(len(mm)),
		uintptr(syscall.MS_ASYNC))
	if err != 0 {
		panic(err)
	}
}*/

/*// NOTE: NOT USED
func (mm mmap) _Mremap(size int) mmap {
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
}*/

/*// NOTE: NOT USED
func _Open(path string) (*os.File, string, int) {
	fd, err := os.OpenFile(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	fi, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	return fd, sanitize(fi.Name()), int(fi.Size())
}*/

/*// NOTE: NOT USED
func _sanitize(path string) string {
	if path[len(path)-1] == '/' {
		return path[:len(path)-1]
	}
	if x := strings.Index(path, "."); x != -1 {
		return path[:x]
	}
	return path
}*/

/*func align(size int) int {
	if size > 0 {
		return (size + page - 1) &^ (page - 1)
	}
	return page
}*/

/*func resize(fd uintptr, size int) int {
	err := syscall.Ftruncate(int(fd), int64(align(size)))
	if err != nil {
		panic(err)
	}
	return size
}*/

/*
// NOTE: WORKS BUT NOT PERFORMANT FOR KEEPING REFERENCE
func (mm mmap) Len() int {
	return len(mm) / page
}
*/

/*
// NOTE: WORKS BUT NOT PERFORMANT FOR KEEPING REFERENCE
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
*/

/*
// NOTE: WORKS BUT NOT PERFORMANT FOR KEEPING REFERENCE
func (mm mmap) Swap(i, j int) {
	pi, pj := i*page, j*page
	copy(temp, mm[pi:pi+page])
	copy(mm[pi:pi+page], mm[pj:pj+page])
	copy(mm[pj:pj+page], temp)
}
*/
