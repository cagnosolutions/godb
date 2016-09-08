package godb

import (
	"fmt"
	"testing"
)

// count should print count... duh
var tree *btree
var data = func(s string, args ...interface{}) []byte {
	return []byte(fmt.Sprintf(s, args...))
}

func Benchmark_BTree_Has(b *testing.B) {
	b.StopTimer()
	tree = NewBTree()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		if tree.Has(d) {
			// should be empty tree
			b.FailNow()
		}
	}
	b.StopTimer()
	tree.Close()
}

func Benchmark_BTree_Add(b *testing.B) {
	b.StopTimer()
	tree = NewBTree()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		tree.Add(d, d)
	}
	b.StopTimer()
	tree.Close()
}

func Benchmark_BTree_Set(b *testing.B) {
	b.StopTimer()
	tree = NewBTree()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		tree.Set(d, d)
	}
	b.StopTimer()
	tree.Close()
}

func Benchmark_BTree_Get(b *testing.B) {
	tree = NewBTree()
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		tree.Set(d, d)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		if tree.Get(d) == nil {
			b.FailNow()
		}
	}
	b.StopTimer()
	tree.Close()
}

func Benchmark_BTree_Del(b *testing.B) {
	tree = NewBTree()
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		tree.Set(d, d)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		tree.Del(d)
	}
	b.StopTimer()
	tree.Close()
}
