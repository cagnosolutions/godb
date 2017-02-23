package godb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"unsafe"
)

// database engine
type engine struct {
	file *os.File
	data []byte
	page int
	zero []byte
	//maxKey = 24
	//maxVal = page - maxKey - 1 // (-1 is for EOF) 4071
}

func createEmptyFile(path string, size int) error {
	fd, err := os.Create(path)
	if err != nil {
		return err
	}
	if err = fd.Truncate(int64(size)); err != nil {
		return err
	}
	if err = fd.Close(); err != nil {
		return err
	}
	return nil
}

func (e *engine) open(path string) (bool, error) {
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

// create and return a new record, returning a non-nil error if
// there is an issue with the key or val check
func (e *engine) newRecord() *record {

}

// add a new record to the engine at the first available slot
// return a non-nil error if there is an issue growing the file
func (e *engine) addRecord(r *record) (int, error) {
	// initialize block position k at beginning of mapped file, as well as future byte offset
	var k, o int
	// start iterating through mapped file reigon one page at a time
	for o < len(e.data) {
		// checking for empty page
		if bytes.Equal(e.data[o:o+e.page], e.zero) {
			// found an empty page, re-use it; copy data into it
			copy(e.data[o:o+e.page], r.data)
			// return location of block in page offset
			return o / e.page, nil
		}
		// go to next page offset
		k++
		o = k * e.page
	}
	// haven't found any empty pages, so let's grow the file
	if err := e.grow(); err != nil {
		return -1, err
	}
	// write.data to page
	copy(e.data[o:o+e.page], r.data)
	// return location of block in page offset
	return o / e.page, nil
}

// update a record at provided offset, assuming one exists
// return a non-nil error if offset is outside of mapped reigon
func (e *engine) setRecord(k int, r *record) error {
	// get byte offset from block position k
	o := k * e.page
	// do a bounds check; if outside of mapped reigon...
	if o+e.page > len(e.data) {
		// do not grow, return an error
		return fmt.Errorf("engine[set]: cannot update record at block %d (offset %d)\n", k, o)
	}
	// wipe page in case updated data is smaller than original dataset
	copy(e.data[o:o+e.page], e.zero)
	// write updated data to page
	copy(e.data[o:o+e.page], r.data)
	// there were no errors, so return nil
	return nil
}

var ErrEmptyRecord error = errors.New("engine: empty record found")
var ErrEngineEOF error = io.EOF

// return a record at provided offset, assuming one exists
// return a non-nil error if offset is outside of mapped reigon
func (e *engine) getRecord(k int) (*record, error) {
	// get byte offset from block position k
	o := k * e.page
	// do a bounds check; if outside of mapped reigon...
	if o+e.page > len(e.data) {
		// ...return an error
		return nil, ErrEngineEOF //fmt.Errorf("engine[get]: cannot return record at block %d (offset %d)\n", k, o)
	}
	// create record to return
	r := new(record)
	// fill out record data if not empty, returning no error
	if n := bytes.LastIndexByte(e.data[o+maxKey-1:o+e.page], eofVal); n > 0 {
		r.data = e.data[o : o+n]
		return r, nil
	}
	// otherwise, return empty record, with an error
	return r, ErrEmptyRecord //fmt.Errorf("engine[get]: empty record found at block %d (offset %d)", k, o)
}

func (e *engine) getRecordKey(k int) ([]byte, error) {
	// get byte offset from block position k
	o := k * e.page
	// do a bounds check; if outside of mapped reigon...
	if o+e.page > len(e.data) {
		// ...return an error
		return nil, fmt.Errorf("engine[getKey]: cannot return key at block %d (offset %d)\n", k, o)
	}
	if !bytes.Equal(e.data[o:o+page], e.zero) {
		return e.data[o : o+maxKey], nil
	}
	// otherwise, return empty record, with an error
	return nil, fmt.Errorf("engine[getKey]: empty key found at block %d (offset %d)", k, o)
}

func (e *engine) getRecordVal(k int) ([]byte, error) {
	// get byte offset from block position k
	o := k * e.page
	// do a bounds check; if outside of mapped reigon...
	if o+e.page > len(e.data) {
		// ...return an error
		return nil, fmt.Errorf("engine[getVal]: cannot return val at block %d (offset %d)\n", k, o)
	}
	// fill out record data if not empty, returning no error
	if n := bytes.LastIndexByte(e.data[o+maxKey:o+e.page], eofVal); n > 0 {
		v := e.data[o+maxKey : o+maxKey+n]
		return v, nil
	}
	// otherwise, return empty record, with an error
	return nil, fmt.Errorf("engine[getVal]: empty val found at block %d (offset %d)", k, o)
}

// delete a record at provided offset, assuming one exists
// return a non-nil error if offset is outside of mapped reigon
func (e *engine) delRecord(k int) error {
	// get byte offset from block position k
	o := k * e.page
	// do a bounds check; if outside of mapped reigon...
	if o+e.page > len(e.data) {
		// ...return an error
		return fmt.Errorf("engine[del]: cannot delete record at block %d (offset %d)\n", k, o)
	}
	// otherwise, wipe page block at offset
	copy(e.data[o:o+page], e.zero)
	// there were no errors, so return nil
	return nil
}

// grow the underlying mapped file
func (e *engine) grow() error {
	// resize the size to double the current, ie. len * 2
	size := ((len(e.data) * 2) + e.page - 1) &^ (e.page - 1)
	// unmap current mapping before growing underlying file...
	e.munmap()
	// truncate underlying file to updated size, check for errors
	if err := syscall.Ftruncate(int(e.file.Fd()), int64(size)); err != nil {
		return err
	}
	// remap underlying file now that it has grown
	e.data = mmap(e.file, 0, size)
	// there were no errors, so return nil
	return nil
}

// close the engine, return any errors encountered
func (e *engine) close() error {
	e.munmap()                             // unmap memory mappings (Munmap automatically flushes)
	if err := e.file.Close(); err != nil { // close underlying file
		return err
	}
	e.file = nil // set file descriptor to nil
	// there were no errors, so return nil
	return nil
}

// temp structure
type payload struct {
	key []byte
	pos int
}

// get all of the record data payloads from the engine
func (e *engine) loadAllRecords() <-chan payload {
	// initialize the channels to return the keys and blocks on
	loader := make(chan payload)
	go func() {
		var o, k int
		// start iterating through mapped file reigon one page at a time
		for o < len(e.data) {
			// checking for non-empty page
			if !bytes.Equal(e.data[o:o+e.page], e.zero) {
				// found one; return key and block offset
				loader <- payload{e.data[o : o+maxKey], (o / e.page)}
			}
			k++
			o = k * e.page
		}
		close(loader)
	}()
	return loader
}

/* +--------------------+ //
// | MEMORY MAPPED FILE | //
// +--------------------+ */

const (
	PROT  uint = syscall.PROT_READ | syscall.PROT_WRITE
	FLAGS uint = syscall.MAP_SHARED
)

func mmap(fd *os.File, off, len int) []byte {
	mm, err := mmapat(0, fd.Fd(), int64(off), int64(len), PROT, FLAGS)
	if err != nil {
		panic(err)
	}
	return mm
}

func mmapat(addr uintptr, fd uintptr, offset, length int64, prot uint, flags uint) ([]byte, error) {
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
	mm := *new([]byte)
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&mm))
	dh.Data = addr
	dh.Len = int(length) // umm... truncating here feels like trouble??
	dh.Cap = dh.Len
	return mm, nil
}

func (e *engine) munmap() {
	dh := *(*reflect.SliceHeader)(unsafe.Pointer(&e.data))
	_, _, err := syscall.Syscall(syscall.SYS_MUNMAP, uintptr(dh.Data), uintptr(dh.Len), 0)
	if err != 0 {
		panic(err)
	}
}

func (e *engine) msync() {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&e.data))
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, uintptr(rh.Data), uintptr(rh.Len), uintptr(syscall.MS_ASYNC))
	if err != 0 {
		panic(err)
	}
}

func (e *engine) misresident() ([]bool, error) {
	sz := os.Getpagesize()                                 // page size
	re := make([]bool, (len(e.data)+sz-1)/sz)              // result
	dh := *(*reflect.SliceHeader)(unsafe.Pointer(&e.data)) // result data ptr
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&re))     // result data header ptr
	_, _, err := syscall.Syscall(syscall.SYS_MINCORE, uintptr(dh.Data), uintptr(dh.Len), uintptr(rh.Data))
	for i := range re {
		*(*uint8)(unsafe.Pointer(&re[i])) &= 1
	}
	if err != 0 {
		return nil, err
	}
	return re, nil
}
