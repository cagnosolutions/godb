package ngin

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
)

// slab constants
const (
	slab_16m = (1 << 24)
	slab_32m = (1 << 25)
	slab_64m = (1 << 26)
)

// page constants
const (
	page_4k = (1 << 12)
	page_8k = (1 << 13)
)

// globals
var (
	slab = slab_64m
	page = page_4k
	temp = make([]byte, page)
)

type engine struct {
	file *os.File
	//indx *btree
	//free *btree
	data mmap
}

func OpenEngine(path string) *engine {
	_, err := os.Stat(path + `.dat`)
	// new instance
	if err != nil && !os.IsExist(err) {
		dirs, _ := filepath.Split(path)
		err := os.MkdirAll(dirs, 0755)
		if err != nil {
			panic(err)
		}
		fd, err := os.Create(path + `.dat`)
		if err != nil {
			panic(err)
		}
		if err := fd.Truncate(int64(slab)); err != nil {
			panic(err)
		}
		if err := fd.Close(); err != nil {
			panic(err)
		}
	}
	// existing
	fd, err := os.OpenFile(path+`.dat`, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	info, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	return &engine{
		file: fd,
		data: Mmap(fd, 0, int(info.Size())),
	}
}

func (e *engine) Set(d []byte, k int) {
	e.set(d, k)
}

func (e *engine) set(d []byte, k int) {
	o := k * page
	if o+page >= len(e.data) {
		e.grow()
	}
	copy(e.data[o:], append(d, make([]byte, (page-len(d)))...))
}

func (e *engine) Get(k int) []byte {
	return e.get(k)
}

func (e *engine) get(k int) []byte {
	o := k * page
	if e.data[o] != 0x00 {
		if n := bytes.IndexByte(e.data[o:o+page], byte(0x00)); n > -1 {
			return e.data[o : o+n]
		}
	}
	return nil
}

func (e *engine) Del(k int) {
	e.del(k)
}

func (e *engine) del(k int) {
	o := k * page
	copy(e.data[o:], temp)
}

func (e *engine) grow() {
	size := ((len(e.data) + slab) + page - 1) &^ (page - 1)
	e.data.Munmap()
	if err := syscall.Ftruncate(int(e.file.Fd()), int64(size)); err != nil {
		panic(err)
	}
	e.data = Mmap(e.file, 0, size)
}

func (e *engine) CloseEngine() {
	e.data.Sync()
	e.data.Munmap()
	e.file.Close()
}
