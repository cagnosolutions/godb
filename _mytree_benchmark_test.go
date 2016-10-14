package godb

import (
	"fmt"
	"testing"
)

func Benchmark_MyTree_Insert(b *testing.B) {
	t := new(btree)

	// insert b.N times
	for n := 0; n < b.N; n++ {
		value := []byte(fmt.Sprintf("test%d", n))
		t.Add(key_t(value), value)
		if t.count != n {
			fmt.Printf("error: count is off %d should be %d\n\n", t.count, n)
		}
	}
}

func Benchmark_MyTree_InsertFind(b *testing.B) {
	t := new(btree)

	// insert b.N times
	for n := 0; n < b.N; n++ {
		value := key_t(fmt.Sprintf("test%d", n))
		t.Add(key_t(value), []byte(value))
		if t.count != n {
			fmt.Printf("error: count is off %d should be %d\n\n", t.count, n)
		}
	}

	// find one by one
	for n := 0; n < b.N; n++ {
		if v := t.Get(key_t(fmt.Sprintf("test%d", n))); v == nil {
			fmt.Printf("error: %s\n\n", v)
		}
	}
}

func Benchmark_MyTree_InsertDelete(b *testing.B) {
	t := new(btree)

	// insert b.N times
	for n := 0; n < b.N; n++ {
		value := fmt.Sprintf("test%d", n)
		t.Add(key_t(value), []byte(value))
		if t.count != n {
			fmt.Printf("error: count is off %d should be %d\n\n", t.count, n)
		}
	}

	// delete them
	for n := 0; n < b.N; n++ {
		c := t.count
		t.Del(key_t(fmt.Sprintf("test%d", n)))
		if t.count >= c {
			fmt.Printf("error: count is off %d should be %d\n\n", t.count, c)
		}
	}
}
