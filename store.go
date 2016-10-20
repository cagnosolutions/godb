package godb

import (
	"bytes"
	"encoding/binary"
	"sync"
)

type store struct {
	buf *bytes.Buffer
	sync.RWMutex
}

func Newstore() *store {
	return &store{
		buf: bytes.NewBuffer(make([]byte, 24, 24)),
	}
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
	s.Lock()
	if err := binary.Write(s.buf, binary.BigEndian, k); err != nil {
		s.buf.Reset()
		s.Unlock()
		return nil, err
	}
	var key []byte
	if s.buf.Len() > 24 {
		s.buf.Truncate(24)
	}
	copy(key[24-s.buf.Len():], s.buf.Bytes())
	s.buf.Reset()
	s.Unlock()
	return key, nil
}

func (s *store) Add(k, v interface{}) error {
	_, err := s.genKey(k)
	if err != nil {
		return err
	}
	return nil
}

func (s *store) Put(k, v interface{}) error {
	_, err := s.genKey(k)
	if err != nil {
		return err
	}
	return nil
}

func (s *store) Get(k, ptr interface{}) error {
	_, err := s.genKey(k)
	if err != nil {
		return err
	}
	return nil
}

func (s *store) Del(k interface{}) error {
	_, err := s.genKey(k)
	if err != nil {
		return err
	}
	return nil
}
