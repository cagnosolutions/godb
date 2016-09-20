package godb

import (
	"bytes"
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

func Test_BTree_IsEmpty(t *testing.T) {
	tree = new(btree)
	if ok := tree.Has([]byte{0x42}); ok {
		t.Fatalf("expexted nil, got: %v\n", ok)
	}
}

func Test_BTree_Get0(t *testing.T) {
	tree = new(btree)
	if count := tree.Count(); count > 0 {
		t.Fatalf("expected -1 or 0, got: %d\n", count)
	}
	if dat := tree.Get([]byte{0x42}); dat != nil {
		t.Fatalf("expexted nil, got: %s\n", dat)
	}
}

func Test_BTree_SetGet0(t *testing.T) {
	tree := new(btree)

	tree.Set([]byte{0x42}, []byte{0x99})
	if count, is := tree.Count(), 1; count != is {
		t.Fatal(count, is)
	}
	if dat := tree.Get([]byte{0x42}); !bytes.Equal(dat, []byte{0x99}) {
		t.Fatalf("expected '0x99', got: %s\n", dat)
	}

	tree.Set([]byte{0x42}, []byte{0x77})
	if count, is := tree.Count(), 1; count != is {
		t.Fatal(count, is)
	}
	if dat := tree.Get([]byte{0x42}); !bytes.Equal(dat, []byte{0x77}) {
		t.Fatalf("expected '0x77', got: %s\n", dat)
	}

	tree.Set([]byte{0x22}, []byte{0x44})
	if count, is := tree.Count(), 2; count != is {
		t.Fatal(count, is)
	}
	if dat := tree.Get([]byte{0x22}); !bytes.Equal(dat, []byte{0x44}) {
		t.Fatalf("expected '0x44', got: %s\n", dat)
	}
}

// btree set sequential
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

// btree set random
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
			kv := strconv.Itoa(v)
			tree.Set([]byte(kv), []byte(kv))
		}
		b.StopTimer()
		tree.Close()
	}
	b.StopTimer()
}

// btree get sequential
func Benchmark_BTree_GetSeq1e3(b *testing.B) {
	benchmark_BTree_GetSeq(b, 1e3)
}

func Benchmark_BTree_GetSeq1e4(b *testing.B) {
	benchmark_BTree_GetSeq(b, 1e4)
}

func Benchmark_BTree_GetSeq1e5(b *testing.B) {
	benchmark_BTree_GetSeq(b, 1e5)
}

func Benchmark_BTree_GetSeq1e6(b *testing.B) {
	benchmark_BTree_GetSeq(b, 1e6)
}

func benchmark_BTree_GetSeq(b *testing.B, n int) {
	tree := new(btree)
	for i := 0; i < n; i++ {
		kv := strconv.Itoa(i)
		tree.Set([]byte(kv), []byte(kv))
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			kv := strconv.Itoa(j)
			if dat := tree.Get([]byte(kv)); !bytes.Equal(dat, []byte(kv)) {
				b.Fatalf("expected a result (0x01), but got: %+#v\n", kv)
			}
		}
	}
	b.StopTimer()
	tree.Close()
}

// btree get random
func Benchmark_BTree_GetRnd1e3(b *testing.B) {
	benchmark_BTree_GetRnd(b, 1e3)
}

func Benchmark_BTree_GetRnd1e4(b *testing.B) {
	benchmark_BTree_GetRnd(b, 1e4)
}

func Benchmark_BTree_GetRnd1e5(b *testing.B) {
	benchmark_BTree_GetRnd(b, 1e5)
}

func Benchmark_BTree_GetRnd1e6(b *testing.B) {
	benchmark_BTree_GetRnd(b, 1e6)
}

func benchmark_BTree_GetRnd(b *testing.B, n int) {
	tree := new(btree)
	a := rand.New(rand.NewSource(37189)).Perm(n)
	for _, v := range a { // fill tree with random data
		kv := strconv.Itoa(v)
		tree.Set([]byte(kv), []byte(kv))
	}
	debug.FreeOSMemory() // free memory, run gc
	b.ResetTimer()       // and reset timer
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			kv := strconv.Itoa(v)
			if dat := tree.Get([]byte(kv)); !bytes.Equal(dat, []byte(kv)) {
				b.Fatalf("expected a result (0x01), but got: %+#v\n", kv)
			}
		}
	}
	b.StopTimer() // stop the timer and close tree.
	tree.Close()
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
