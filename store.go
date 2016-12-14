package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/cagnosolutions/godb/msgpack"
)

type Store struct {
	store
}

func OpenStore(path string) (*Store, error) {
	s, err := openStore(path)
	return &Store{store: *s}, err
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

type store struct {
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
		idx: idx,
		buf: bytes.NewBuffer(make([]byte, maxKey, maxKey)),
	}
	st.buf.Reset()
	return st, nil
}

func (s *store) Add(key, val interface{}) error {
	s.Lock()
	defer s.Unlock()
	k, err := s.genKey(key)
	if err != nil {
		return fmt.Errorf("store[add]: error while generating key -> %q", err)
	}
	//v, err := json.Marshal(val)
	v, err := msgpack.Marshal(val)
	if err != nil {
		return fmt.Errorf("store[add]: error while attempting to marshal -> %q", err)
	}
	if err := verify(k, v); err != nil {
		return fmt.Errorf("store[add]: error while doing bounds check -> %q", err)
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
	//v, err := json.Marshal(val)
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
	//if err := json.Unmarshal(v, ptr); err != nil {
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

func (s *store) QueryOne(qry string, ptr interface{}) error {
	s.RLock()
	defer s.RUnlock()
	qrys := strings.Split(qry, "&&")
	var vals [][]byte
	for _, q := range qrys {
		vals = append(vals, <-s.qry(q))
	}
	if len(vals) != 1 {
		return fmt.Errorf("error: query returned more or less than one result (%d)\n", len(vals))
	}
	if err := msgpack.Unmarshal(vals[0], ptr); err != nil {
		return err
	}
	return nil
}

func (s *store) Query(qry string, ptr interface{}) error {
	s.RLock()
	defer s.RUnlock()
	qrys := strings.Split(qry, "&&")
	var vals [][]byte
	for _, q := range qrys {
		vals = append(vals, <-s.qry(q))
	}
	res := make([]interface{}, len(vals))
	for n, v := range vals {
		if err := msgpack.Unmarshal(v, res[n]); err != nil {
			return err
		}
	}
	b, err := msgpack.Marshal(res)
	if err != nil {
		return err
	}
	if err := msgpack.Unmarshal(b, ptr); err != nil {
		return err
	}
	return nil
}

// *NO LOCKS!
func (s *store) qry(q string) <-chan []byte {
	var dec *msgpack.Decoder
	var buf *bytes.Buffer
	ch := make(chan []byte)
	go func() {
		for val := range s.idx.next() {
			if val == nil {
				break
			}
			buf = bytes.NewBuffer(val)
			dec = msgpack.NewDecoder(buf)
			ok, err := dec.Query(q)
			if err != nil {
				ch <- []byte(err.Error())
			}
			if ok {
				ch <- val
			}
		}
		close(ch)
	}()
	return ch
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
		return fmt.Errorf("store[verify]: val exceeds maximum val length of %d bytes, by %d bytes\n", maxVal, len(val)-maxVal)
	}
	// passed bounds check, no errors so return nil
	return nil
}

func (s *store) genKeyMsgpack(k interface{}) ([]byte, error) {
	b, err := msgpack.Marshal(k)
	if err != nil {
		return nil, err
	}
	if len(b) > maxKey {
		b = b[:maxKey] // truncate to len of maxKey
		return b, nil
	}
	b = append(make([]byte, maxKey-len(b)), b...)
	return b, nil
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
