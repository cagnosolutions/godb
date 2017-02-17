package godb

import (
    "sync"
    "log"
)

type DB struct {
    *store
    sync.RWMutex
}

func OpenDB() *DB {
    return &DB{
        store : func() *store {
            st, err := openStore()
            if err != nil {
                log.Fatal(err)
            }
            return st
        }()
    }
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

var (
	ErrKind error = errors.New("invalid kind")
	ErrType error = errors.New("invalid type")
)

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
		f := typ.Field(i)
		fmt.Printf("%d: %s %s = %v\n", i,
			f.Name, f.Type, f.Tag)
	}

	fmt.Println(val, typ)
	return nil
}

func CloseDB(db *DB) error {
    return db.store.close()
}

