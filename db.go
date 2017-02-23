package godb

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// errors
const ErrKind error = errors.New("invalid kind: expected a pointer to a struct")

type DB struct {
	*store
	sync.RWMutex
}

func OpenDB() (*DB, error) {
	st, err := openStore()
	if err != nil {
		return nil, err
	}
	return &DB{st}, nil
}

func (db *DB) Insert(ptr interface{}) error {

	return nil
}

func (db *DB) Return(qry string, ptr interface{}) error {

	return nil
}

func (db *DB) Update(qry string, ptr interface{}) error {

	return nil
}

func (db *DB) Delete(qry string) error {

	return nil
}

func assign(v interface{}) error {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr {
		return ErrKind
	}
	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return ErrKind
	}
	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		sf, vf := typ.Field(i), val.Field(i)
		if tag, ok := sf.Tag.Lookup("db"); ok {
			if tag == "_id" && vf.Kind() == reflect.Int && vf.CanSet() {
				vf.SetInt(time.Now().UnixNano())
			}
		}
	}

	fmt.Println(val, typ)
	return nil
}

func CloseDB(db *DB) error {
	return db.store.close()
}
