package godb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
)

type store struct {
	idx *btree
	buf *bytes.Buffer
	sync.RWMutex
}

func OpenStore(path string) (*store, error) {
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
	v, err := json.Marshal(val)
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
	v, err := json.Marshal(val)
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
	fmt.Printf("###############--> %s\n", v)
	if err := json.Unmarshal(v, ptr); err != nil {
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
