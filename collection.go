package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/cagnosolutions/msgpack"
)

type Collection struct {
	st  *store
	dsn string
	buf *bytes.Buffer
	sync.RWMutex
}

func OpenCollection(path string) (*Collection, error) {
	st, err := openStore(path)
	if err != nil {
		return nil, err
	}
	c := &Collection{
		st:  st,
		dsn: path,
		buf: bytes.NewBuffer(make([]byte, maxKey, maxKey)),
	}
	c.buf.Reset()
	return c, nil
}

// log error to console; then return error
func logger(err error) error {
	if err != nil {
		log.Println(err)
	}
	return err
}

func verify(key, val []byte) error {
	if len(key) > maxKey {
		return fmt.Errorf("verify: key exceeds maximum key length of %d bytes, by %d bytes\n", maxKey, len(key)-maxKey)
	}
	if len(val) > maxVal {
		return ErrPageSize // trigger disk page grow
	}
	return nil
}

// do a bounds check and return size of marshaled value
func (c *Collection) boundscheck(key, val interface{}) ([]byte, []byte, error) {
	k, err := c.genKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("collection: error while generating key (%q)", err)
	}
	v, err := msgpack.Marshal(val)
	if err != nil {
		return nil, nil, fmt.Errorf("collection: error while attempting to marshal (%q)", err)
	}
	if err := verify(k, v); err != nil {
		if err != ErrPageSize {
			return nil, nil, fmt.Errorf("collection: error while veryifying data (%q)", err)
		}
		return k, v, err
	}
	return k, v, nil
}

func (c *Collection) genKey(k interface{}) ([]byte, error) {
	switch k.(type) {
	case string:
		k = []byte(k.(string))
	case int:
		k = int64(k.(int))
	case uint:
		k = uint64(k.(uint))
	}
	if err := binary.Write(c.buf, binary.BigEndian, k); err != nil {
		c.buf.Reset()
		return nil, err
	}
	key := make([]byte, maxKey, maxKey)
	if c.buf.Len() > maxKey {
		c.buf.Truncate(maxKey)
	}
	copy(key[maxKey-c.buf.Len():], c.buf.Bytes())
	c.buf.Reset()
	return key, nil
}

// align to nearest 4KB chunk
func align(size int) int {
	if size > 0 {
		return (size + (1 << 12) - 1) &^ ((1 << 12) - 1)
	}
	return 1 << 12 // 4KB
}

// grow the page size on disk
func (c *Collection) growPageSizeOnDisk(valsz int) error {
	c.Lock()
	defer c.Unlock()
	// new page size
	ps := align(valsz)
	// create new index using new page size
	if err := createEmptyFile(c.dsn+`_.ix`, ps); err != nil {
		return err
	}
	// new file size
	fs := c.st.count() * ps
	// create new file using new page size * number of current records
	if err := createEmptyFile(c.dsn+`_.db`, fs); err != nil {
		return err
	}
	// create new engine
	en := new(engine)
	// open path to new "grown" engine
	if _, err := en.open(c.dsn + `_`); err != nil {
		// something went wrong; clean up temp files
		if err := os.Remove(c.dsn + `_.ix`); err != nil {
			return err
		}
		if err := os.Remove(c.dsn + `_.db`); err != nil {
			return err
		}
		return err
	}
	// iterate memory mapped records, add them to the new store
	for pos := 0; true; pos++ {
		rec, err := c.st.getRecordFromEngine(pos)
		if err != nil {
			if err == ErrEngineEOF {
				break
			} else if err == ErrEmptyRecord {
				continue
			}
		}
		// no errors, so add record directly to the new engine (ignore page offset addRecord returns)
		if _, err := en.addRecord(rec); err != nil {
			return err
		}
	}
	// close new store so everything syncs
	if err := en.close(); err != nil {
		return err
	}
	// close the existing store and set to nil
	if err := c.st.close(); err != nil {
		return err
	}
	// set current store to nil now that is's closed
	c.st = nil
	// next remove existing store files
	if err := os.Remove(c.dsn + `.ix`); err != nil {
		return err
	}
	if err := os.Remove(c.dsn + `.db`); err != nil {
		return err
	}
	// rename new engine files
	if err := os.Rename(c.dsn+`_.ix`, c.dsn+`.ix`); err != nil {
		return err
	}
	if err := os.Rename(c.dsn+`_.db`, c.dsn+`.db`); err != nil {
		return err
	}
	// reopen store now utilizing new "grown" engine
	s, err := openStore(c.dsn)
	if err != nil {
		return err
	}
	// force a garbage collect before we re-open the store
	runtime.GC()
	// re-assign new "grown" store to this collection
	c.st = s
	// everything went fine, so return a nil error
	return nil
}

func (c *Collection) Add(key, val interface{}) error {
	// generate key and val, also bounds and grow check
	k, v, err := c.boundscheck(key, val)
	if err != nil {
		if err == ErrPageSize {
			// grow underlying file and proceed (method locks on its own)
			if err := c.growPageSizeOnDisk(len(v)); err != nil {
				return logger(err)
			}
		} else {
			return logger(err)
		}
	}
	c.Lock()
	err = c.st.add(k, v)
	c.Unlock()
	return logger(err)
}

func (c *Collection) Set(key, val interface{}) error {
	// generate key and val, also bounds and grow check
	k, v, err := c.boundscheck(key, val)
	if err != nil {
		if err == ErrPageSize {
			// grow underlying file and proceed (method locks on its own)
			if err := c.growPageSizeOnDisk(len(v)); err != nil {
				return logger(err)
			}
		} else {
			return logger(err)
		}
	}
	c.Lock()
	err = c.st.set(k, v)
	c.Unlock()
	return logger(err)
}

func (c *Collection) Get(key, ptr interface{}) error {
	k, err := c.genKey(key)
	if err != nil {
		return logger(err)
	}
	c.RLock()
	err = c.st.get(k, ptr)
	c.RUnlock()
	return logger(err)
}

func (c *Collection) Del(key interface{}) error {
	k, err := c.genKey(key)
	if err != nil {
		return logger(err)
	}
	c.Lock()
	err = c.st.del(k)
	c.Unlock()
	return logger(err)
}

func (c *Collection) All(ptr interface{}) error {
	c.RLock()
	err := c.st.all(ptr)
	c.RUnlock()
	return logger(err)
}

func (c *Collection) Query(qry string, ptr interface{}) error {
	c.RLock()
	err := c.st.query(qry, ptr)
	c.RUnlock()
	return logger(err)
}

func (c *Collection) Count() int {
	c.RLock()
	n := c.st.count()
	log.Println(n)
	c.RUnlock()
	return n
}

func (c *Collection) Close() error {
	c.Lock()
	err := c.st.close()
	c.Unlock()
	return logger(err)
}
