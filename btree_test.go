package godb

import (
	"fmt"
	"math/rand"
	"runtime/debug"
	"strconv"
	"testing"
)

// count should print count... duh
var tree *btree
var data = func(s string, args ...interface{}) []byte {
	return []byte(fmt.Sprintf(s, args...))
}

// TESTS

func Test_Btree_IsEmpty(t *testing.T) {
	tree = new(btree)
	if ok := tree.Has([]byte{0x42}); ok {
		t.Fatalf("expexted nil, got: %v\n", ok)
	}
}

func Test_Btree_GetNonExistantKey(t *testing.T) {
	tree = new(btree)
	if count := tree.Count(); count > 0 {
		t.Fatalf("expected -1 or 0, got: %d\n", count)
	}
	if dat := tree.Get([]byte{0x42}); dat != nil {
		t.Fatalf("expexted nil, got: %s\n", dat)
	}
}

// BENCHMARKS

func Benchmark_BTree_SetSeq_1e3(b *testing.B) {
	benchmark_BTree_SetSeq(b, 1e3)
}

func Benchmark_BTree_SetSeq_1e4(b *testing.B) {
	benchmark_BTree_SetSeq(b, 1e4)
}

func Benchmark_BTree_SetSeq_1e5(b *testing.B) {
	benchmark_BTree_SetSeq(b, 1e5)
}

func Benchmark_BTree_SetSeq_1e6(b *testing.B) {
	benchmark_BTree_SetSeq(b, 1e6)
}

func benchmark_BTree_SetSeq(b *testing.B, n int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := new(btree)
		debug.FreeOSMemory()
		b.StartTimer()
		for j := 0; j < n; j++ {
			tree.Set([]byte(strconv.Itoa(j)), []byte{0x01})
		}
		b.StopTimer()
		tree.Close()
	}
	b.StopTimer()
}

func Benchmark_BTree_SetRnd_1e3(b *testing.B) {
	benchmark_BTree_SetRnd(b, 1e3)
}

func Benchmark_BTree_SetRnd_1e4(b *testing.B) {
	benchmark_BTree_SetRnd(b, 1e4)
}

func Benchmark_BTree_SetRnd_1e5(b *testing.B) {
	benchmark_BTree_SetRnd(b, 1e5)
}

func Benchmark_BTree_SetRnd_1e6(b *testing.B) {
	benchmark_BTree_SetRnd(b, 1e6)
}

func benchmark_BTree_SetRnd(b *testing.B, n int) {
	a := rand.New(rand.NewSource(37189)).Perm(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := new(btree)
		debug.FreeOSMemory()
		b.StartTimer()
		for _, v := range a {
			tree.Set([]byte(strconv.Itoa(v)), []byte{0x01})
		}
		b.StopTimer()
		tree.Close()
	}
	b.StopTimer()
}

// OTHER TESTING....
func Benchmark_BTree_Has(b *testing.B) {
	b.StopTimer()
	tree = new(btree)
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
	tree = new(btree)
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
	tree = new(btree)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		tree.Set(d, d)
	}
	b.StopTimer()
	tree.Close()
}

func Benchmark_BTree_Get(b *testing.B) {
	tree = new(btree)
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
	tree = new(btree)
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
