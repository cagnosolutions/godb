package godb

import (
	"bytes"
	"encoding/binary"
	"sync"
)

type Store struct {
	buf *bytes.Buffer
	sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		buf: bytes.NewBuffer(make([]byte, 24, 24)),
	}
}

func (s *Store) genKey(k interface{}) (key_t, error) {
	switch k.(type) {
	case string:
		k = []byte(k.(string))
	case int:
		k = int64(k.(int))
	case uint:
		k = uint64(k.(uint))
	}
	s.Lock()
	if err := binary.Write(s.buf, binary.BigEndian, k); err != nil {
		s.buf.Reset()
		s.Unlock()
		return key_t_zero, err
	}
	var key key_t
	if s.buf.Len() > 24 {
		s.buf.Truncate(24)
	}
	copy(key[24-s.buf.Len():], s.buf.Bytes())
	s.buf.Reset()
	s.Unlock()
	return key, nil
}

func (s *Store) Add(k, v interface{}) error {
	_, err := s.genKey(k)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) Put(k, v interface{}) error {
	_, err := s.genKey(k)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) Get(k, ptr interface{}) error {
	_, err := s.genKey(k)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) Del(k interface{}) error {
	_, err := s.genKey(k)
	if err != nil {
		return err
	}
	return nil
}
