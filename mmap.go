package godb

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"unsafe"
)

const (
	PROT  uint = syscall.PROT_READ | syscall.PROT_WRITE
	FLAGS uint = syscall.MAP_SHARED
)

type mmap []byte

func Mmap(fd *os.File, off, len int) mmap {
	mm, err := mmap_at(0, fd.Fd(), int64(off), int64(len), PROT, FLAGS)
	if err != nil {
		panic(err)
	}
	return mm
}

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

func (mm mmap) Fd() *os.File {
	fd := uintptr(unsafe.Pointer(&mm[0]))

}

func Open(path string) (bool, error) {
	// check to make sure engine is not already open
	if e.file != nil {
		// return an error if it is
		return true, fmt.Errorf("engine[open]: engine is already open at path %q\n", path)
	}
	_, err := os.Stat(path + `.db`)
	var fdstat bool
	// new instance
	if err != nil && !os.IsExist(err) {
		fdstat = true
		dirs, _ := filepath.Split(path)
		err = os.MkdirAll(dirs, 0755) // 0800
		if err != nil {
			return fdstat, err
		}
		// create new database file with initial size of 2MB
		err = createEmptyFile(path+`.db`, (1 << 21))
		if err != nil {
			return fdstat, err
		}
		// create new meta file with initial record size of 16KB
		err = createEmptyFile(path+`.ix`, (1 << 14))
		if err != nil {
			return fdstat, err
		}
	}
	// existing
	fd, err := os.OpenFile(path+`.db`, os.O_RDWR|os.O_APPEND, 0666) // 0800
	if err != nil {
		return fdstat, err
	}
	info, err := fd.Stat()
	if err != nil {
		return fdstat, err
	}
	// map file into virtual address space and set up engine
	e.file = fd
	e.data = mmap(fd, 0, int(info.Size()))
	// read meta file and set page/block size
	info, err = os.Stat(path + `.ix`)
	if err != nil {
		return fdstat, err
	}
	// set / reassign page size
	e.page = int(info.Size())
	// set / reassign maxVal size
	maxVal = page - maxKey - 1
	// set / reassign empty block
	e.zero = make([]byte, e.page)
	// there were no errors, so return mapped size and a nil error
	return fdstat, nil
}
func OpenFile(path string) (*os.File, string, int) {

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

/*// NOTE: WORKS BUT NOT PERFORMANT FOR KEEPING REFERENCE
func (mm mmap) Len() int {
	return len(mm) / page
}*/

/*// NOTE: WORKS BUT NOT PERFORMANT FOR KEEPING REFERENCE
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

/*// NOTE: WORKS BUT NOT PERFORMANT FOR KEEPING REFERENCE
func (mm mmap) Swap(i, j int) {
	pi, pj := i*page, j*page
	copy(temp, mm[pi:pi+page])
	copy(mm[pi:pi+page], mm[pj:pj+page])
	copy(mm[pj:pj+page], temp)
}*/
