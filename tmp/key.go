package godb

import "sync"

type key_tt [24]byte

var newK = sync.Pool{New: func() interface{} {
	return *new(key_tt)
}}

func key(b []byte) key_tt {
	k := newK.Get().(key_tt)
	copy(k[:13], b[:13])
	copy(k[13:], b[13:len(b)+13])
	return k
}
