package godb

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
)

/*
    NOTE
    ====
    After much thought and
    conceptuial testing in
    python, there is no reason
    to keep the data blocks 
    ordered on disk. it will 
    take a significant amount 
    of time to implement and 
    test and there is only
    one single benefit of doing
    so. that is, the ability to
    binary search the data without
    the use of the tree. this would
    mainly be useful during queries,
    and since we dont even know what
    those would look like yet then 
    i dont see this as being the most
    pragmatic approach. 
    
    instead, the engine should have a 
    newRecord() (*record, int) method 
    that either finds an empty slot 
    and returns a *record and offset
    to the underlying mapped file, or
    returns one from the end. in addition
    the engine should have methods such as
    getRecord(n int) *record, as well as
    putRecord(n int, *record) that readd
    the record off of disk at offset n, and
    writes a record to offset n. the tree
    can then be used in its origional state
    (for the most part) as a primary index
    to read and write records to disk via the
    engine, without regatd to orsering. the
    leaf node pointers will still point to 
    the correct locations of said records.
 */

type engine struct {
	file *os.File
	//indx *btree
	//free *btree
	data mmap
}

func (e *engine) newRecord(key, val []byte) (*record, int) {
    k, o := 0, -1
    for o := k * page; o < len(e.mmap); k++ {
        if e.data[o] == 0x00 {
            return &record{key, val}, o
        }
    }
    e.grow()
    return &record{key, val}, o
}

func (e *engine) getRecord(k offset) *record {
	   o := k * page
	   if e.data[o] != 0x00 {
	       if n := bytes.IndexByte(e.data[o:o+page], byte(0x00)); n > -1 {
		      	    return e.data[o : o+n]
		      	    key := e.data[o : o+25]
		      	    j := bytes.IndexByte(e.data[o+25:o+25+n], 0x00)
		      	    if j == -1 {
		      	        return nil
		      	    }
		      	    val := e.data[o+25 : j]
		      	    return &record{key, val}
		      }
	   }
	   return nil
}

func (e *engine) putRecord(k offset, r *record) {
    o := k * page
	    // do a bounds check, grow if nessicary...
    if o+page >= len(e.data) {
	        e.grow()
	    }
	    d := append(r.key, r.val...)
	    // copy the data `one-off` the offset
	    copy(e.data[o:], append(d, make([]byte, (page-len(d)))...)
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
