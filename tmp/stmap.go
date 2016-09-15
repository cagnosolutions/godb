package godb

import "sort"

// Sorted Tree Map
type STMap struct {
	m map[string][]byte
	s []string
}

func NewSTMap() *STMap {
	return &STMap{
		m: make(map[string][]byte, 0),
		s: make([]string, 0),
	}
}

func (stm *STMap) Has(key string) bool {
	_, ok := stm.m[key]
	return ok
}

// safe; only add new entry if it doesn't already exist
func (stm *STMap) Add(key string, val []byte) {
	if _, ok := stm.m[key]; !ok {
		stm.m[key] = val
		stm.s = append(stm.s, key)
	}
}

// unsafe; add or updates an entry
func (stm *STMap) Set(key string, val []byte) {
	if _, ok := stm.m[key]; ok {
		// key exists; so update / overwrite entry
		i := sort.SearchStrings(stm.s, key)
		stm.s[i] = key
	} else {
		// key doesn't exist; so add new entry
		stm.s = append(stm.s, key)
	}
	stm.m[key] = val // update / insert into map
}

// return value by key
func (stm *STMap) Get(key string) []byte {
	if val, ok := stm.m[key]; ok {
		// key exists; return value
		return val
	}
	return nil
}

// delete entry by key
func (stm *STMap) Del(key string) {
	if _, ok := stm.m[key]; ok {
		// key exists; delete from map
		delete(stm.m, key)
		// check if sorted
		if !sort.StringsAreSorted(stm.s) {
			// lets sort them
			sort.Strings(stm.s)
		}
		// find index of key in sorted list
		i := sort.SearchStrings(stm.s, key)
		// once we find the index, delete it
		copy(stm.s[i:], stm.s[i+1:])
		stm.s[len(stm.s)-1] = ""
		stm.s = stm.s[:len(stm.s)-1]
	}
}

// return all entries in sorted order
func (stm *STMap) All() (all [][]byte) {
	// check if sorted
	if !sort.StringsAreSorted(stm.s) {
		// lets sort them
		sort.Strings(stm.s)
	}
	// create value collection
	var vals [][]byte
	// loop sorted key slice
	for _, key := range stm.s {
		vals = append(vals, stm.m[key])
	}
	return vals
}

// return total number of entries
func (stm *STMap) Count() int {
	return len(stm.s)
}

// remove all entries; close
func (stm *STMap) Close() {
	for k, _ := range stm.m {
		stm.Del(k)
	}
	stm.m, stm.s = nil, nil
}
