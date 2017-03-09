package godb

import (
	"encoding/json"
	"io"
	"os"
)

type Store struct {
	engine ngn
	index  idx
	coder  cdr
}

type idx interface{}

type cdr interface {
	Encode(v interface{}) error
	Decode(v interface{}) error
}

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

type MyEngine struct {
	*os.File
	*json.Decoder
	*json.Encoder
}

/*func NewMyEngine() *MyEngine {
	f, _ := os.Open("")
	return &MyEngine{
		f,
		json.NewDecoder(bytes.NewBuffer([]byte{})),
		json.NewEncoder(bytes.NewBuffer([]byte{})),
	}
}*/

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

func (s *Store) Select() {

	NewStore(MyEngine{})
}
