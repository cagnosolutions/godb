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

func assign(ptr interface{}) error {

    return nil
}

func CloseDB(db *DB) error {
    return db.store.close()
}

