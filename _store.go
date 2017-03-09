package godb

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/cagnosolutions/godb/msgpack"
)

type store struct {
	//dsn string
	idx *btree
	//buf *bytes.Buffer
}

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//		OPEN A STORE		//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
func openStore(path string) (*store, error) {
	idx := &btree{ngin: new(engine)}
	if err := idx.open(path); err != nil {
		return nil, err
	}
	return &store{idx}, nil
	/*
		st := &store{
			dsn: path,
			idx: idx,
			buf: bytes.NewBuffer(make([]byte, maxKey, maxKey)),
		}
		st.buf.Reset()
		return st, nil
	*/
}

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//		KEY GENERATOR		//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
/*
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
*/

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//		BOUNDS CHECKER		//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
var ErrPageSize = errors.New("val too large for current page/block size")

/*
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
*/

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//			ADD				//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
func (s *store) add(key []byte, val []byte) error {
	if err := s.idx.add(key, val); err != nil {
		return fmt.Errorf("store[add]: error while adding to index -> %q", err)
	}
	return nil
}

/*
func (s *store) add(key, val interface{}) error {
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
		return ErrPageSize
	}
	if err := s.idx.add(k, v); err != nil {
		return fmt.Errorf("store[add]: error while adding to index -> %q", err)
	}
	return nil
}
*/

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//			SET				//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
func (s *store) set(key []byte, val []byte) error {
	if err := s.idx.set(key, val); err != nil {
		return fmt.Errorf("store[set]: error while adding to index -> %q", err)
	}
	return nil
}

/*
func (s *store) set(key, val interface{}) error {
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
*/

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//			GET				//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
func (s *store) get(key []byte, ptr interface{}) error {
	v, err := s.idx.get(key)
	if err != nil {
		return fmt.Errorf("store[get]: error while getting value from index -> %q", err)
	}
	if err := msgpack.Unmarshal(v, ptr); err != nil {
		return fmt.Errorf("store[get]: error while attempting to un-marshal -> %q", err)
	}
	return nil
}

/*
func (s *store) get(key, ptr interface{}) error {
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
*/

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//			DEL				//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
func (s *store) del(key []byte) error {
	if err := s.idx.del(key); err != nil {
		return fmt.Errorf("store[del]: error while deleting value from index -> %q", err)
	}
	return nil
}

/*
func (s *store) del(key interface{}) error {
	k, err := s.genKey(key)
	if err != nil {
		return fmt.Errorf("store[del]: error while generating key -> %q", err)
	}
	if err := s.idx.del(k); err != nil {
		return fmt.Errorf("store[del]: error while deleting value from index -> %q", err)
	}
	return nil
}
*/

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//			GETALL			//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
func (s *store) all(ptr interface{}) error {
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

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//			QUERY			//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
func (s *store) query(qry string, ptr interface{}) error {

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

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//		QUERY-MATCHER		//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
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

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//	RETURN RECORD FROM NGIN	//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
func (s *store) getRecordFromEngine(pos int) (*record, error) {
	return s.idx.ngin.getRecord(pos)
}

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//		RETURN COUNT		//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
func (s *store) count() int {
	return s.idx.count
}

/*	=~=~=~=~=~=~=~=~=~=~=~=	//
//		 CLOSE STORE		//
//	=~=~=~=~=~=~=~=~=~=~=~=	*/
func (s *store) close() error {
	return s.idx.close()
}

/*
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
		rec, err := s.idx.ngin.getRecord(pos)
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
	// close the existing store's index
	if err := s.idx.close(); err != nil {
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
	s.idx = &btree{ngin: new(engine)}
	if err := s.idx.open(s.dsn); err != nil {
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
*/
