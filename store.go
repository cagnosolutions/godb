package godb

import (
	"io"
	"sync"
)

type Db struct {
	Stores map[string]*Store
	sync.RWMutex
}

func (d *Db) GetStore(store string) *Store {
	d.RLock()
	st := d.Stores[store]
	d.RUnlock()
	return st
}

func getStore(name string, engine ngn, index idx, marsh mrsh) *Store {
	return &Store{
		engine:     engine,
		index:      index,
		marshaller: marsh,
	}
}

type Store struct {
	engine     ngn
	index      idx
	marshaller mrsh
}

type mrsh interface {
	Marshal() (text []byte, err error)
	Unmarshal(text []byte) error
}

type idx interface{}

type ngn interface {
	io.Reader
	io.Writer
	io.Closer
	io.WriterAt
	io.ReaderAt
	io.Seeker
}

func NewStore(n ngn) *Store {
	return &Store{
		engine: n,
	}
}

func (s *Store) Insert(r *record) (int, error) {
	return 0, nil
}

func (s *Store) Update(k int, r *record) error {
	return nil
}

func (s *Store) Return(k int) (*record, error) {

	return nil, nil
}

func (s *Store) Delete(k int) error {
	return nil
}
