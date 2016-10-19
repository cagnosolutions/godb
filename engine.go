package godb

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
)

type engine struct {
	file *os.File
	//indx *btree
	//free *btree
	data mmap
}

func OpenEngine(path string) *engine {
	_, err := os.Stat(path + `.db`)
	// new instance
	if err != nil && !os.IsExist(err) {
		dirs, _ := filepath.Split(path)
		err := os.MkdirAll(dirs, 0755)
		if err != nil {
			panic(err)
		}
		fd, err := os.Create(path + `.db`)
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
	fd, err := os.OpenFile(path+`.db`, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	info, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	// map file into virtual address space
	return &engine{
		file: fd,
		data: Mmap(fd, 0, int(info.Size())),
	}
}

func (e *engine) set(d []byte, k int) {
	// get byte offset from position k
	o := k * page
	// do a bounds check, grow if nessicary...
	if o+page >= len(e.data) {
		e.grow()
	}
	// copy the data `one-off` the offset
	copy(e.data[o:], append(d, make([]byte, (page-len(d)))...))
}

func (e *engine) get(k int) []byte {
	// get byte offset from position k
	o := k * page
	if e.data[o] != 0x00 {
		if n := bytes.IndexByte(e.data[o:o+page], byte(0x00)); n > -1 {
			return e.data[o : o+n]
		}
	}
	return nil
}

func (e *engine) del(k int) {
	// get byte offset from position k
	o := k * page
	// copy number of pages * page size worth
	// of nil bytes starting at the k's offset
	copy(e.data[o:], temp)
}

func (e *engine) grow() {
	// resize size to current size + 16MB chunk (grow in 16 MB chunks)
	size := ((len(e.data) + slab) + page - 1) &^ (page - 1)
	// unmap current mapping before growing underlying file...
	e.data.Munmap()
	// truncate underlying file to updated size, check for errors
	if err := syscall.Ftruncate(int(e.file.Fd()), int64(size)); err != nil {
		panic(err)
	}
	// remap underlying file now that it has grown
	e.data = Mmap(e.file, 0, size)
}

func (e *engine) CloseEngine() {
	e.data.Sync()   // flush data to disk
	e.data.Munmap() // unmap memory mappings
	e.file.Close()  // close underlying file
}
