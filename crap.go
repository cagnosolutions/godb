package godb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
)

const SLAB int64 = (1 << 22) // 4MB

type Doc struct {
	Off int
	Len int
}

type Engine struct {
	File *os.File
	Docs map[string]*Doc
	Mmap mmap
}

func OpenEngine(path string) *Engine {
	engine := &Engine{
		File: OpenFile(path),
		Docs: make(map[string]*Doc, 0),
	}
	// memory map file and return engine
	engine.Mmap = MapFile(engine.File)
	return engine
}

func CloseEngine(e *Engine) error {
	return nil
}

func (e *Engine) Insert(k []byte, v []byte) error {
	if _, exists := e.Docs[string(k)]; exists {
		return errors.New("insert: document with that key already exists!")
	}

	data := make([]byte, 20)
	binary.PutVarint(data[:10], int64(len(k)))
	binary.PutVarint(data[10:], int64(len(v)))

	data = append(data, append(k, v...)...)

	off, grow := e.findEmpty(len(data))
	if grow {
		e.grow()
	}

	e.Mmap.writeAt(data, off)
	e.Docs[string(k)] = &Doc{off, len(data)}

	return nil
}

func (e *Engine) Update(k, v []byte) {

}

func (e *Engine) Return(k []byte) []byte {

	return nil
}

func (e *Engine) Delete(k []byte) {

}

func OpenFile(path string) *os.File {
	_, err := os.Stat(path + `.db`)
	// check new
	if err != nil && !os.IsExist(err) {
		dirs, _ := filepath.Split(path)
		if err := os.MkdirAll(dirs, 0755); err != nil {
			panic(err)
		}
		// create new
		fd, err := os.Create(path + `.db`)
		if err != nil {
			panic(err)
		}
		if err := fd.Truncate(int64(SLAB)); err != nil {
			panic(err)
		}
		// return new file
		return fd
	}
	// existing
	fd, err := os.OpenFile(path+`.db`, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	// return existing file
	return fd
}

func GetFileInfo(fd *os.File) os.FileInfo {
	info, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	return info
}
