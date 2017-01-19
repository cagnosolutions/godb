package godb

import (
	"fmt"
	"os"
	"syscall"
	"time"
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

// linux_amd64
func mmap_syscall(addr, length, prot, flags, fd uintptr, offset int64) (uintptr, error) {
	addr, _, err := syscall.Syscall6(syscall.SYS_MMAP, addr, length, prot, flags, fd, uintptr(offset))
	return addr, err
}

func Map(addr uintptr, fd uintptr, offset, length int64, prot ProtFlags, flags MapFlags) (mmap, error) {
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
	dh.Len = int(length) // Hmmm.. truncating here feels like trouble.
	dh.Cap = dh.Len
	return mm, nil
}


func (mm mmap) Unmap() error {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mm))
	_, _, err := syscall.Syscall(syscall.SYS_MUNMAP, uintptr(rh.Data), uintptr(rh.Len), 0)
	if err != 0 {
		return err
	}
	return nil
}

func (mm mmap) Sync(flags SyncFlags) error {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mm))
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, uintptr(rh.Data), uintptr(rh.Len), uintptr(flags))
	if err != 0 {
		return err
	}
	return nil
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
	t1 := time.Now().UnixNano()
	err := syscall.Munmap(mm)
	t2 := time.Now().UnixNano()
	fmt.Printf("syscall.Munmap(mm):\n\tnanoseconds: %d\n\tmicroseconds: %d\n\tmilliseconds: %d\n\n", t2-t1, (t2-t1)/1000, ((t2-t1)/1000)/1000)
	mm = nil
	if err != nil {
		panic(err)
	}
}

func (mm mmap) Flush() {
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

/*func Open(path string) (*os.File, string, int) {
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
}*/

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

// func

//  works but not preforment keeping for reference

/*func (mm mmap) Len() int {
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
}*/
