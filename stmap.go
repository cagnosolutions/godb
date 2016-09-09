package godb

import "sort"

// Sorted Tree Map
type STMap struct {
	m map[string][]byte
	s sort.StringSlice
}

func NewSTMap() *STMap {
	return &STMap{
		m: make(map[string][]byte, 0),
		s: make(sort.StringSlice, 0),
	}
}

func (stm *STMap) Has(key string) bool {
	_, ok := stm.m[key]
	return ok
}

func (stm *STMap) Add(key string, val []byte) {
	// only add new entry if it doesn't already exist
	if _, ok := stm.m[key]; !ok {
		stm.s = append(stm.s, key)
		stm.m[key] = val
	}
	stm.s.Sort() // sort the index
}

func (stm *STMap) Set(key string, val []byte) {
	if _, ok := stm.m[key]; ok {
		// update
		i := stm.s.Search(key)
		stm.s[i] = key
	} else {
		// insert
		stm.s = append(stm.s, key)
	}
	stm.m[key] = val // update/insert into map
	stm.s.Sort()     // sort the index
}

func (stm *STMap) Get(key string) []byte {
	if val, ok := stm.m[key]; ok {
		return val
	}
	return nil
}

func (stm *STMap) Del(key string) {
	if _, ok := stm.m[key]; ok {
		// if it exists, delte from map
		delete(stm.m, key)

		// find index of key in sorted list
		stm.s.Sort()
		i := stm.s.Search(key)
		// once we find the index, delete it...
		copy(stm.s[i:], stm.s[i+1:])
		stm.s[len(stm.s)-1] = ""
		stm.s = stm.s[:len(stm.s)-1]
		// then resort
		stm.s.Sort()
	}
}

func (stm *STMap) All() (all [][]byte) {
	stm.s.Sort()
	for i := 0; i < len(stm.m); i++ {
		key := stm.s[i]
		if _, ok := stm.m[key]; !ok {
			continue
		}
		all = append(all, stm.m[key])
	}
	return
}

func (stm *STMap) Count() int {
	return len(stm.m)
}

func (stm *STMap) Close() {
	for k, _ := range stm.m {
		stm.Del(k)
	}
}
