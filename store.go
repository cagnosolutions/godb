package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/cagnosolutions/godb/msgpack"
)

/*
// NOTE: Turn Store struct with embedded store into a Store interface.
type Store interface {
	func OpenStore(path string) (*Store, error)
	func (s *Store) Add(key, val interface{}) error
	func (s *Store) Set(key, val interface{}) error
	func (s *Store) Get(key, ptr interface{}) error
	func (s *Store) Del(key interface{}) error
	func (s *Store) QueryOne(qry string, ptr interface{}) error
	func (s *Store) Query(qry string, ptr interface{}) error
	func (s *Store) Count() int
	func (s *Store) Close() error
	func (s *Store) Sync()
}
*/

var ErrPageSize = errors.New("val too large for current page/block size")

type Store struct {
	*store
}

func OpenStore(path string) (*Store, error) {
	s, err := openStore(path)
	return &Store{store: s}, err
}

func (s *Store) Add(key, val interface{}) error {
	return s.store.Add(key, val)
}

func (s *Store) Set(key, val interface{}) error {
	return s.store.Set(key, val)
}

func (s *Store) Get(key, ptr interface{}) error {
	return s.store.Get(key, ptr)
}

func (s *Store) Del(key interface{}) error {
	return s.store.Del(key)
}

func (s *Store) QueryOne(qry string, ptr interface{}) error {
	return s.store.QueryOne(qry, ptr)
}

func (s *Store) Query(qry string, ptr interface{}) error {
	return s.store.Query(qry, ptr)
}

func (s *Store) Count() int {
	return s.store.Count()
}

func (s *Store) Close() error {
	return s.store.Close()
}

func (s *Store) Sync() {
	s.Lock()
	defer s.Unlock()
	s.store.idx.ngin.data.Sync()
}

type store struct {
	dsn string
	idx *btree
	buf *bytes.Buffer
	sync.RWMutex
}

func openStore(path string) (*store, error) {
	idx := &btree{ngin: new(engine)}
	if err := idx.open(path); err != nil {
		return nil, err
	}
	st := &store{
		dsn: path,
		idx: idx,
		buf: bytes.NewBuffer(make([]byte, maxKey, maxKey)),
	}
	st.buf.Reset()
	return st, nil
}

// NOTE: WORK IN PROGRESS...
/*func (s *store) Cmd(str string, ptr ...interface{}) error {
	var obj interface{}
	if len(ptr) > 0 {
		obj = ptr[0]
	}
	argv := strings.Fields(str)
	var key, val interface{}
	switch argv[0] {
	case "ADD":
		return s.Add(key, val)
	case "SET":
		return s.Set(key, val)
	case "GET":
		return s.Get(key, val)
	case "DELETE":
		return s.Del(key)
	case "QUERY":
		if v, ok := val.(string); ok {
			return s.Query(v, obj)
		}
		return fmt.Errorf("val is not a string")
	}
	return nil
}*/

func (s *store) growPageSizeOnDisk(valsz int) error {
	// new page size
	ps := align(valsz)
	// create new index using new page size
	if err := createEmptyFile(s.dsn+`_.ix`, ps); err != nil {
		return err
	}
	// new file size
	fs := s.idx.count * ps
	// create new file using new page size * number of current records
	if err := createEmptyFile(s.dsn+`_.db`, fs); err != nil {
		return err
	}
	// create new engine
	en := new(engine)
	// open path to new "grown" engine
	if _, err := en.open(s.dsn + `_`); err != nil {
		// something went wrong; clean up temp files
		if err := os.Remove(s.dsn + `_.ix`); err != nil {
			return err
		}
		if err := os.Remove(s.dsn + `_.db`); err != nil {
			return err
		}
		return err
	}
	// iterate memory mapped records, add them to the new store
	for pos := 0; true; pos++ {
		rec, err := en.getRecord(pos)
		if err != nil {
			// if record is empty continue (skip), otherwise return err
			if rec == nil {
				continue
			}
			return err
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
	// close the existing store
	if err := s.Close(); err != nil {
		return err
	}
	// next remove existing store files
	if err := os.Remove(s.dsn + `.ix`); err != nil {
		return err
	}
	if err := os.Remove(s.dsn + `.db`); err != nil {
		return err
	}
	// rename new engine files
	if err := os.Rename(s.dsn+`_.ix`, s.dsn+`.ix`); err != nil {
		return err
	}
	if err := os.Rename(s.dsn+`_.db`, s.dsn+`.db`); err != nil {
		return err
	}
	// reopen current store now utilizing new "grown" engine
	s, err := openStore(s.dsn)
	if err != nil {
		return err
	}
	// everything went fine, so return a nil error
	return nil
}

// align to nearest 4KB chunk
func align(size int) int {
	if size > 0 {
		return (size + (1 << 12) - 1) &^ ((1 << 12) - 1)
	}
	return 1 << 12 // 4KB
}

func (s *store) Add(key, val interface{}) error {
	s.Lock()
	defer s.Unlock()
	k, err := s.genKey(key)
	if err != nil {
		return fmt.Errorf("store[add]: error while generating key -> %q", err)
	}
	v, err := msgpack.Marshal(val)
	if err != nil {
		return fmt.Errorf("store[add]: error while attempting to marshal -> %q", err)
	}
	if err := verify(k, v); err != nil {
		if err != ErrPageSize {
			return fmt.Errorf("store[add]: error while doing bounds check -> %q", err)
		}
		// handle page grow
		if err := s.growPageSizeOnDisk(len(v)); err != nil {
			return err
		}
	}
	if err := s.idx.add(k, v); err != nil {
		return fmt.Errorf("store[add]: error while adding to index -> %q", err)
	}
	return nil
}

func (s *store) Set(key, val interface{}) error {
	s.Lock()
	defer s.Unlock()
	k, err := s.genKey(key)
	if err != nil {
		return fmt.Errorf("store[set]: error while generating key -> %q", err)
	}
	v, err := msgpack.Marshal(val)
	if err != nil {
		return fmt.Errorf("store[set]: error while attempting to marshal -> %q", err)
	}
	if err := verify(k, v); err != nil {
		return fmt.Errorf("store[set]: error while doing bounds check -> %q", err)
	}
	if err := s.idx.set(k, v); err != nil {
		return fmt.Errorf("store[set]: error while setting in index -> %q", err)
	}
	return nil
}

func (s *store) Get(key, ptr interface{}) error {
	s.RLock()
	defer s.RUnlock()
	k, err := s.genKey(key)
	if err != nil {
		return fmt.Errorf("store[get]: error while generating key -> %q", err)
	}
	v, err := s.idx.get(k)
	if err != nil {
		return fmt.Errorf("store[get]: error while getting value from index -> %q", err)
	}
	if err := msgpack.Unmarshal(v, ptr); err != nil {
		return fmt.Errorf("store[get]: error while attempting to un-marshal -> %q", err)
	}
	return nil
}

func (s *store) Del(key interface{}) error {
	s.Lock()
	defer s.Unlock()
	k, err := s.genKey(key)
	if err != nil {
		return fmt.Errorf("store[del]: error while generating key -> %q", err)
	}
	if err := s.idx.del(k); err != nil {
		return fmt.Errorf("store[del]: error while deleting value from index -> %q", err)
	}
	return nil
}

func (s *store) All(ptr interface{}) error {
	typ := reflect.TypeOf(ptr)
	if typ.Kind() != reflect.Ptr {
		return fmt.Errorf("error: expected pointer to model\n")
	}

	// derefrencing pointer; getting model type and value
	typ = typ.Elem()
	val := reflect.Indirect(reflect.ValueOf(ptr))

	// init vars and split query values
	var dec *msgpack.Decoder
	var buf *bytes.Reader

	for rec := range s.idx.nextRecord() {
		if rec == nil {
			continue
		}
		// fill out buffer, and initialize decoder
		buf = bytes.NewReader(rec)
		dec = msgpack.NewDecoder(buf)

		// new pointer to refect value of single ptr type
		zro := reflect.Indirect(reflect.New(typ.Elem()))
		if err := dec.DecodeValue(zro); err != nil {
			return err
		}
		// append value to ptr value
		val.Set(reflect.Append(val, zro))
	}
	return nil
}

func (s *store) QueryOne(qry string, ptr interface{}) error {
	return nil
}

func (s *store) Query(qry string, ptr interface{}) error {

	// type checking for pointer
	typ := reflect.TypeOf(ptr)
	if typ.Kind() != reflect.Ptr {
		return fmt.Errorf("error: expected pointer to model\n")
	}

	// derefrencing pointer; getting model type and value
	typ = typ.Elem()
	val := reflect.Indirect(reflect.ValueOf(ptr))

	// init vars and split query values
	var dec *msgpack.Decoder
	var buf *bytes.Reader
	qrys := strings.Split(qry, "&&")

	// iterate the index by record, skipping empty ones
	for rec := range s.idx.nextRecord() {
		if rec == nil {
			continue
		}
		// fill out buffer, and initialize decoder
		buf = bytes.NewReader(rec)
		dec = msgpack.NewDecoder(buf)

		// check for a query match
		ok, err := match(dec, qrys)
		if err != nil {
			return err
		}

		// found a match!
		if ok {
			// new pointer to refect value of single ptr type
			zro := reflect.Indirect(reflect.New(typ.Elem()))
			if err := dec.DecodeValue(zro); err != nil {
				return err
			}
			// append matched value to ptr value
			val.Set(reflect.Append(val, zro))
		}
	}
	return nil
}

func match(dec *msgpack.Decoder, qrys []string) (bool, error) {
	for _, qry := range qrys {
		ok, err := dec.Query(qry)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		// rewind the decoder's bytes reader
		if err := dec.Rewind(); err != nil {
			return false, err
		}

	}
	return true, nil
}

func (s *store) Count() int {
	s.RLock()
	defer s.RUnlock()
	return s.idx.count
}

func (s *store) Close() error {
	s.Lock()
	defer s.Unlock()
	return s.idx.close()
}

// do verify by doing bounds check
func verify(key, val []byte) error {
	// key bounds check
	if len(key) > maxKey {
		return fmt.Errorf("store[verify]: key exceeds maximum key length of %d bytes, by %d bytes\n", maxKey, len(key)-maxKey)
	}
	// val bounds check
	if len(val) > maxVal {
		//return fmt.Errorf("store[verify]: val exceeds maximum val length of %d bytes, by %d bytes\n", maxVal, len(val)-maxVal)
		// trigger page/block grow
		return ErrPageSize
	}
	// passed bounds check, no errors so return nil
	return nil
}

func (s *store) genKey(k interface{}) ([]byte, error) {
	switch k.(type) {
	case string:
		k = []byte(k.(string))
	case int:
		k = int64(k.(int))
	case uint:
		k = uint64(k.(uint))
	}
	if err := binary.Write(s.buf, binary.BigEndian, k); err != nil {
		s.buf.Reset()
		return nil, err
	}
	key := make([]byte, maxKey, maxKey)
	if s.buf.Len() > maxKey {
		s.buf.Truncate(maxKey)
	}
	copy(key[maxKey-s.buf.Len():], s.buf.Bytes())
	s.buf.Reset()
	return key, nil
}
