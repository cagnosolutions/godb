package godb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

var (
	page  = (1 << 12)     //   4 KB
	slab  = (1 << 19) * 8 // 512 KB * 8 == 4 MB
	empty = make([]byte, page, page)
)

/*// database engine interface
type dbEngine interface {
	open(path string) (int, error)    // return size of open mapped file or an error encountered while opening engine
	addRecord(r *record) (int, error) // return a block page offset or non-nil error if there is an issue growing the file
	setRecord(k int, r *record) error // return a non-nil error if offset is outside of mapped reigon
	getRecord(k int) (*record, error) // return a record at provided offset or a non-nil error if offset is outside of mapped reigon
	delRecord(k int) error            // return a non-nil error if offset is outside of mapped reigon
	grow() error                      // return a non-nil error if there was an issue growing the file
	close() error                     // return any errors encountered while closing the engine
}*/

// database engine
type engine struct {
	file *os.File
	data mmap
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
		// create new database file
		err = createEmptyFile(path+`.db`, slab)
		if err != nil {
			return fdstat, err
		}
		// create new meta file
		err = createEmptyFile(path+`.ix`, page)
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
	e.data = Mmap(fd, 0, int(info.Size()))
	// read meta file and set page/block size
	info, err = os.Stat(path + `.ix`)
	if err != nil {
		return fdstat, err
	}
	// set page/block size
	page = int(info.Size())
	// there were no errors, so return mapped size and a nil error
	return fdstat, nil
}

// add a new record to the engine at the first available slot
// return a non-nil error if there is an issue growing the file
func (e *engine) addRecord(r *record) (int, error) {
	// initialize block position k at beginning of mapped file, as well as future byte offset
	var k, o int
	// start iterating through mapped file reigon one page at a time
	for o < len(e.data) {
		// checking for empty page
		if bytes.Equal(e.data[o:o+page], empty) {
			// if e.data[o+maxKey-1] == 0x00 {
			// found an empty page, re-use it; copy data into it
			copy(e.data[o:o+page], r.data)
			// return location of block in page offset
			return o / page, nil
		}
		// go to next page offset
		k++
		o = k * page
	}
	// haven't found any empty pages, so let's grow the file
	if err := e.grow(); err != nil {
		return -1, err
	}
	// write data to page
	copy(e.data[o:o+page], r.data)
	// return location of block in page offset
	return o / page, nil
}

// update a record at provided offset, assuming one exists
// return a non-nil error if offset is outside of mapped reigon
func (e *engine) setRecord(k int, r *record) error {
	// get byte offset from block position k
	o := k * page
	// do a bounds check; if outside of mapped reigon...
	if o+page > len(e.data) {
		// do not grow, return an error
		return fmt.Errorf("engine[set]: cannot update record at block %d (offset %d)\n", k, o)
	}
	// wipe page in case updated data is smaller than original dataset
	copy(e.data[o:o+page], empty)
	// write updated data to page
	copy(e.data[o:o+page], r.data)
	// there were no errors, so return nil
	return nil
}

// return a record at provided offset, assuming one exists
// return a non-nil error if offset is outside of mapped reigon
func (e *engine) getRecord(k int) (*record, error) {
	// get byte offset from block position k
	o := k * page
	// do a bounds check; if outside of mapped reigon...
	if o+page > len(e.data) {
		// ...return an error
		return nil, fmt.Errorf("engine[get]: cannot return record at block %d (offset %d)\n", k, o)
	}
	// create record to return
	r := new(record)
	// fill out record data if not empty, returning no error
	if n := bytes.LastIndexByte(e.data[o+maxKey-1:o+page], eofVal); n > 0 {
		r.data = e.data[o : o+n]
		return r, nil
	}
	// otherwise, return empty record, with an error
	return r, fmt.Errorf("engine[get]: empty record found at block %d (offset %d)", k, o)
}

func (e *engine) getRecordKey(k int) ([]byte, error) {
	// get byte offset from block position k
	o := k * page
	// do a bounds check; if outside of mapped reigon...
	if o+page > len(e.data) {
		// ...return an error
		return nil, fmt.Errorf("engine[getKey]: cannot return key at block %d (offset %d)\n", k, o)
	}
	if !bytes.Equal(e.data[o:o+page], empty) {
		return e.data[o : o+maxKey], nil
	}
	// otherwise, return empty record, with an error
	return nil, fmt.Errorf("engine[getKey]: empty key found at block %d (offset %d)", k, o)
}

func (e *engine) getRecordVal(k int) ([]byte, error) {
	// get byte offset from block position k
	o := k * page
	// do a bounds check; if outside of mapped reigon...
	if o+page > len(e.data) {
		// ...return an error
		return nil, fmt.Errorf("engine[getVal]: cannot return val at block %d (offset %d)\n", k, o)
	}
	// fill out record data if not empty, returning no error
	if n := bytes.LastIndexByte(e.data[o+maxKey:o+page], eofVal); n > 0 {
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
	o := k * page
	// do a bounds check; if outside of mapped reigon...
	if o+page > len(e.data) {
		// ...return an error
		return fmt.Errorf("engine[del]: cannot delete record at block %d (offset %d)\n", k, o)
	}
	// otherwise, wipe page block at offset
	copy(e.data[o:o+page], empty)
	// there were no errors, so return nil
	return nil
}

// grow the underlying mapped file
func (e *engine) grow() error {
	// t1 := time.Now().UnixNano()
	// slab *= 2
	// resize the size to double the current, ie. len * 2
	size := ((len(e.data) * 2) + page - 1) &^ (page - 1)
	// unmap current mapping before growing underlying file...
	e.data.Sync()
	e.data.Munmap()
	// truncate underlying file to updated size, check for errors
	if err := syscall.Ftruncate(int(e.file.Fd()), int64(size)); err != nil {
		return err
	}
	// remap underlying file now that it has grown
	e.data = Mmap(e.file, 0, size)
	// there were no errors, so return nil
	// t2 := time.Now().UnixNano()
	// fmt.Printf("engine.grow():\n\tnanoseconds: %d\n\tmicroseconds: %d\n\tmilliseconds: %d\n\n", t2-t1, (t2-t1)/1000, ((t2-t1)/1000)/1000)
	return nil
}

// close the engine, return any errors encountered
func (e *engine) close() error {
	e.data.Sync()                          // flush data to disk
	e.data.Munmap()                        // unmap memory mappings
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

// get all of the record key's from the engine (not nessicarily in order)
func (e *engine) loadAllRecords() <-chan payload {
	// initialize the channels to return the keys and blocks on
	loader := make(chan payload)
	go func() {
		var o, k int
		// start iterating through mapped file reigon one page at a time
		for o < len(e.data) {
			// checking for non-empty page
			if !bytes.Equal(e.data[o:o+page], empty) {
				// found one; return key and block offset
				loader <- payload{e.data[o : o+maxKey], (o / page)}
			}
			k++
			o = k * page
		}
		close(loader)
	}()
	return loader
}
