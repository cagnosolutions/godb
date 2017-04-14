package godb

import (
	"encoding/json"
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

func (e *Engine) Insert(k string, v interface{}) error {
	if _, ok := e.Docs[k]; !ok {
		// check for empty space
		if doc, ok := e.Docs["empty.doc"]; ok {
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			_, err = e.WriteAt(b, doc.Off)
			if err != nil {
				return err
			}
			doc.Len = len(b)
			e.Docs[k] = doc
			delete(e.Docs["empty.doc"])
		}
		// marshal, seek, pack doc, write, etc
		return nil
	}
	return errors.New("insert: document with that key already exists!")
}

func (e *Engine) Update(k string, v interface{}) {

}

func (e *Engine) Return(k string, v interface{}) {

}

func (e *Engine) Delete(k string) {

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

func MapFile(fd *os.File) mmap {
	fi := GetFileInfo(fd)
	mm, err := mmap_at(0, fd.Fd(), 0, fi.Size(), PROT, FLAGS)
	if err != nil {
		panic(err)
	}
	return mm
}

func GetFileInfo(fd *os.File) os.FileInfo {
	info, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	return info
}
