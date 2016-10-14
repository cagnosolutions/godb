package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var (
	keyPool = sync.Pool{New: func() interface{} { return bytes.NewBuffer(make([]byte, 24, 24)) }}
	buf     *bytes.Buffer
)

func Key(k interface{}) (key_t, error) {
	switch k.(type) {
	case string:
		k = []byte(k.(string))
	case int:
		k = int64(k.(int))
	case uint:
		k = uint64(k.(uint))
	}
	buf = keyPool.Get().(*bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, k); err != nil {
		keyPool.Put(buf)
		return key_t_zero, err
	}
	var key key_t
	if buf.Len() > 24 {
		buf.Truncate(24)
	}
	copy(key[24-buf.Len():], buf.Bytes())
	buf.Reset()
	fmt.Println(buf.Cap())
	keyPool.Put(buf)
	return key, nil
}

type StorageEngine interface {
	Put(k []byte, v []byte) error
	// Get(k []byte) (*Record, error)
	Del(k []byte) error
}

type Store struct {
	name  string
	index *btree
	data  *Engine
	sync.RWMutex
}

type Engine struct {
	file *os.File
	data mmap
}

func Open(path string) *Engine {
	// case new
	_, err := os.Stat(path + `.dat`)
	if err != nil && !os.IsExist(err) {
		// create directory path
		dirs, _ := filepath.Split(path)
		err := os.MkdirAll(dirs, 0755)
		if err != nil {
			panic(err)
		}
		// create data file and truncate
		fd, err := os.Create(path + `.dat`)
		if err != nil {
			panic(err)
		}
		// write initial file size (16MB)
		if err := fd.Truncate(1 << 24); err != nil {
			panic(err)
		}
		if err := fd.Close(); err != nil {
			panic(err)
		}
	}
	// open file to construct rest of data structure
	fd, err := os.OpenFile(path+`.dat`, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	info, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	e := &Engine{
		file: fd,
		data: Mmap(fd, 0, int(info.Size())),
	}
	return e
}
