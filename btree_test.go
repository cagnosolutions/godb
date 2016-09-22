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

// data helper
var data = func(s string, args ...interface{}) []byte {
	return []byte(fmt.Sprintf(s, args...))
}

// test has
func Test_BTree_Has(t *testing.T) {
	tree = new(btree)
	if ok := tree.Has([]byte{0x42}); ok {
		t.Fatalf("expexted nil, got: %v\n", ok)
	}
	if count := tree.Count(); count != 0 {
		t.Fatalf("expected 0, got: %v\n", count)
	}
}

// test add
func Test_BTree_Add(t *testing.T) {
	tree := new(btree)
	tree.Add([]byte{0x42}, []byte{0x99})
	if count := tree.Count(); count != 1 {
		t.Fatalf("expected 1, got: %d\n", count) // should be 1
	}
	if dat := tree.Get([]byte{0x42}); !bytes.Equal(dat, []byte{0x99}) {
		t.Fatalf("expected '0x99', got: %s\n", dat)
	}
	tree.Add([]byte{0x42}, []byte{0x77}) // overwrite record, should no work
	if count := tree.Count(); count != 1 {
		t.Fatalf("expected 1, got: %d\n", count) // should be 1
	}
	if dat := tree.Get([]byte{0x42}); !bytes.Equal(dat, []byte{0x99}) {
		t.Fatalf("expected '0x99', got: %s\n", dat)
	}
	tree.Add([]byte{0x22}, []byte{0x44})
	if count := tree.Count(); count != 2 {
		t.Fatalf("expected 2, got: %d\n", count) // should be 2
	}
	if dat := tree.Get([]byte{0x22}); !bytes.Equal(dat, []byte{0x44}) {
		t.Fatalf("expected '0x44', got: %s\n", dat)
	}
}

// test get
func Test_BTree_Get(t *testing.T) {
	tree = new(btree)
	if count := tree.Count(); count != 0 {
		t.Fatalf("expected 0, got: %d\n", count)
	}
	if dat := tree.Get([]byte{0x11}); dat != nil {
		t.Fatalf("expexted nil, got: %s\n", dat)
	}
	tree.Set([]byte{0x11}, []byte{0x01})
	if count := tree.Count(); count != 1 {
		t.Fatalf("expected 1, got: %d\n", count)
	}
	if dat := tree.Get([]byte{0x11}); !bytes.Equal(dat, []byte{0x01}) {
		t.Fatalf("expected '0x01', got: %s\n", dat)
	}
}

// test set
func Test_BTree_Set(t *testing.T) {
	tree := new(btree)
	tree.Set([]byte{0x42}, []byte{0x99})
	if count := tree.Count(); count != 1 {
		t.Fatalf("expected 1, got: %d\n", count) // should be 1
	}
	if dat := tree.Get([]byte{0x42}); !bytes.Equal(dat, []byte{0x99}) {
		t.Fatalf("expected '0x99', got: %s\n", dat)
	}
	tree.Set([]byte{0x42}, []byte{0x77}) // overwrite record
	if count := tree.Count(); count != 1 {
		t.Fatalf("expected 1, got: %d\n", count) // should be 1
	}
	if dat := tree.Get([]byte{0x42}); !bytes.Equal(dat, []byte{0x77}) {
		t.Fatalf("expected '0x77', got: %s\n", dat)
	}
	tree.Set([]byte{0x22}, []byte{0x44})
	if count := tree.Count(); count != 2 {
		t.Fatalf("expected 2, got: %d\n", count) // should be 2
	}
	if dat := tree.Get([]byte{0x22}); !bytes.Equal(dat, []byte{0x44}) {
		t.Fatalf("expected '0x44', got: %s\n", dat)
	}
}

// test del
func Test_BTree_Del(t *testing.T) {
	tree := new(btree)
	tree.Del([]byte{0x11})                 // delete non-existant key
	if count := tree.Count(); count != 0 { // check to make sure count doesn't decrement unnecessarily
		t.Fatalf("expected size=0, got: %d\n", count) // should be 0
	}
	tree.Set([]byte{0x11}, []byte{0x11})   // set key
	tree.Del([]byte{0x01})                 // attempty to delete key that doesn't exist
	if count := tree.Count(); count != 1 { // check to make sure count doesn't decrement unnecessarily
		t.Fatalf("expected size=1, got: %d\n", count) // should be 1
	}
	tree.Set([]byte{0x22}, []byte{0x22})
	if count := tree.Count(); count != 2 { // check to make sure count is correct
		t.Fatalf("expected size=2, got: %d\n", count) // should be 2
	}
	tree.Set([]byte{0x33}, []byte{0x33})   // count=3
	tree.Set([]byte{0x44}, []byte{0x44})   // count=4
	if count := tree.Count(); count != 4 { // check to make sure count is correct
		t.Fatalf("expected size=4, got: %d\n", count) // should be 4
	}
	tree.Del([]byte{0x33})                 // del 0x33, now count=3
	if count := tree.Count(); count != 3 { // check to make sure count doesn't decrement unnecessarily
		t.Fatalf("expected size=3, got: %d\n", count) // should be 3
	}
	tree.Set([]byte{0x55}, []byte{0x55})   // put 0x55, count=4
	tree.Del([]byte{0x11})                 // del 0x11, count=3
	if count := tree.Count(); count != 3 { // check to make sure count is correct
		t.Fatalf("expected size=3, got: %d\n", count)
	}
	tree.Del([]byte{0x44})                 // del 0x44, count=2
	tree.Del([]byte{0x22})                 // del 0x22, count=1
	if count := tree.Count(); count != 1 { // check to make sure count is correct
		t.Fatalf("expected size=1, got: %d\n", count)
	}
	tree.Del([]byte{0x55})                 // del 0x55, count=0
	if count := tree.Count(); count != 0 { // check to make sure count is correct
		t.Fatalf("expected size=0, got: %d\n", count)
	}
}

// btree set sequential
func Benchmark_BTree_SetSeq_A_1e3(b *testing.B) {
	benchmark_BTree_SetSeq_A(b, 1e3)
}

func Benchmark_BTree_SetSeq_A_1e4(b *testing.B) {
	benchmark_BTree_SetSeq_A(b, 1e4)
}

func Benchmark_BTree_SetSeq_A_1e5(b *testing.B) {
	benchmark_BTree_SetSeq_A(b, 1e5)
}

func Benchmark_BTree_SetSeq_A_1e6(b *testing.B) {
	benchmark_BTree_SetSeq_A(b, 1e6)
}

func benchmark_BTree_SetSeq_A(b *testing.B, n int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := new(btree)
		debug.FreeOSMemory()
		b.StartTimer()
		for j := 0; j < n; j++ {
			tree.Set([]byte(strconv.Itoa(j)) /*[]byte{0xde, 0xad, 0xbe, 0xef}*/, []byte{0x01})
		}
		b.StopTimer()
		if count := tree.Count(); count != n {
			b.Fatalf("expected %d entries, got: %d entries instead\n", n, count)
		}
		tree.Close()
	}
	b.StopTimer()
}

func Benchmark_BTree_SetSeq_B_1e3(b *testing.B) {
	benchmark_BTree_SetSeq_B(b, 1e3)
}

func Benchmark_BTree_SetSeq_B_1e4(b *testing.B) {
	benchmark_BTree_SetSeq_B(b, 1e4)
}

func Benchmark_BTree_SetSeq_B_1e5(b *testing.B) {
	benchmark_BTree_SetSeq_B(b, 1e5)
}

func Benchmark_BTree_SetSeq_B_1e6(b *testing.B) {
	benchmark_BTree_SetSeq_B(b, 1e6)
}
func benchmark_BTree_SetSeq_B(b *testing.B, n int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := new(btree)
		debug.FreeOSMemory()
		b.StartTimer()
		for j := 0; j < n; j++ {
			k, v := data("key-%.6d", j), data("val-%.6d", j)
			tree.Set([]byte(k), []byte(v))
		}
		b.StopTimer()
		if count := tree.Count(); count != n {
			b.Fatalf("expected %d entries, got: %d entries instead\n", n, count)
		}
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
		if count := tree.Count(); count != n {
			b.Fatalf("expected %d entries, got: %d entries instead\n", n, count)
		}
		tree.Close()
	}
	b.StopTimer()
}

// btree get sequential
func Benchmark_BTree_GetSeq_1e3(b *testing.B) {
	benchmark_BTree_GetSeq(b, 1e3)
}

func Benchmark_BTree_GetSeq_1e4(b *testing.B) {
	benchmark_BTree_GetSeq(b, 1e4)
}

func Benchmark_BTree_GetSeq_1e5(b *testing.B) {
	benchmark_BTree_GetSeq(b, 1e5)
}

func Benchmark_BTree_GetSeq_1e6(b *testing.B) {
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
				b.Fatalf("expected [(%T) %s], but got: [(%T) %s]\n", kv, kv, dat, dat)
			}
		}
	}
	b.StopTimer()
	tree.Close()
}

// btree get random
func Benchmark_BTree_GetRnd_1e3(b *testing.B) {
	benchmark_BTree_GetRnd(b, 1e3)
}

func Benchmark_BTree_GetRnd_1e4(b *testing.B) {
	benchmark_BTree_GetRnd(b, 1e4)
}

func Benchmark_BTree_GetRnd_1e5(b *testing.B) {
	benchmark_BTree_GetRnd(b, 1e5)
}

func Benchmark_BTree_GetRnd_1e6(b *testing.B) {
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
				b.Fatalf("expected %+#v, but got: %+#v\n", kv, dat)
			}
		}
	}
	b.StopTimer() // stop the timer and close tree.
	tree.Close()
}

// OTHER TESTING....
/*
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
*/
