package godb

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime/debug"
	"strconv"
	"testing"

	"github.com/collinglass/bptree"
)

// count should print count... duh
var tree *bptree.Tree

// data helper
var data = func(s string, args ...interface{}) []byte {
	return []byte(fmt.Sprintf(s, args...))
}

// test has
func Test_BPTree_Has(t *testing.T) {
	tree = bptree.NewTree()
	if _, err := tree.Find(42, false); err != nil {
		t.Fatalf("expexted nil, got: %v\n", err)
	}
	if tree.Root.NumKeys != 0 {
		t.Fatalf("expected 0, got: %v\n", tree.Root.NumKeys)
	}
}

// test add
func Test_BPTree_Add(t *testing.T) {
	tree = bptree.NewTree()
	if err := tree.Insert(42, []byte{0x99}); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
	}
	if tree.Root.NumKeys != 1 {
		t.Fatalf("expected 1, got: %d\n", tree.Root.NumKeys) // should be 1
	}
	if r, err := tree.Find(42, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r, []byte{0x99}) {
			t.Fatalf("expected '0x99', got: %s\n", r)
		}
	}
	if err := tree.Insert(42, []byte{0x77}); err != nil { // overwrite record, should not work
		t.Fatalf("got error while inserting: %v\n", err)
	}
	if tree.Root.NumKeys != 1 {
		t.Fatalf("expected 1, got: %d\n", tree.Root.NumKeys) // should be 1
	}
	if r, err := tree.Find(42, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r, []byte{0x99}) {
			t.Fatalf("expected '0x99', got: %s\n", r)
		}
	}
	if err := tree.Insert(22, []byte{0x44}); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
	}
	if tree.Root.NumKeys != 2 {
		t.Fatalf("expected 2, got: %d\n", tree.Root.NumKeys) // should be 2
	}
	if r, err := tree.Find(22, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r, []byte{0x44}) {
			t.Fatalf("expected '0x44', got: %s\n", r)
		}
	}
}

// test get
func Test_BPTree_Get(t *testing.T) {
	tree = bptree.NewTree()
	if tree.Root.NumKeys != 0 {
		t.Fatalf("expected 0, got: %d\n", tree.Root.NumKeys)
	}
	if r, err := tree.Find(11, false); r != nil {
		t.Fatalf("expexted nil, got: %s\n", r)
	}
	if err := tree.Insert(11, []byte{0x01}); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
	}
	if tree.Root.NumKeys != 1 {
		t.Fatalf("expected 1, got: %d\n", tree.Root.NumKeys)
	}
	if r, err := tree.Find(11, false); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
		if !bytes.Equal(r, []byte{0x01}) {
			t.Fatalf("expected '0x01', got: %s\n", r)
		}
	}
}

// test set
func Test_BPTree_Set(t *testing.T) {
	tree = bptree.NewTree()
	if err := tree.Insert(42, []byte{0x99}); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
	}
	if tree.Root.NumKeys != 1 {
		t.Fatalf("expected 1, got: %d\n", tree.Root.NumKeys) // should be 1
	}
	if r, err := tree.Find(42, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r, []byte{0x99}) {
			t.Fatalf("expected '0x99', got: %s\n", r)
		}
	}
	if err := tree.Insert(42, []byte{0x77}); err != nil { // overwrite record
		t.Fatalf("got error while inserting: %v\n", err)
	}
	if tree.Root.NumKeys != 1 {
		t.Fatalf("expected 1, got: %d\n", tree.Root.NumKeys) // should be 1
	}
	if r, err := tree.Find(42, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r, []byte{0x77}) {
			t.Fatalf("expected '0x77', got: %s\n", r)
		}
	}
	if err := tree.Insert(22, []byte{0x44}); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
	}
	if tree.Root.NumKeys != 2 {
		t.Fatalf("expected 2, got: %d\n", tree.Root.NumKeys) // should be 2
	}
	if r, err := tree.Find(22, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r, []byte{0x44}) {
			t.Fatalf("expected '0x44', got: %s\n", r)
		}
	}
}

// test del
func Test_BPTree_Del(t *testing.T) {
	tree = bptree.NewTree()
	if err := tree.Delete(11); err == nil { // delete nonexistant key
		t.Fatalf("did not get error while attempting to deleting nonexistant key\n")
	}
	if tree.Root.NumKeys != 0 { // check to make sure count doesn't decrement unnecessarily
		t.Fatalf("expected size=0, got: %d\n", tree.Root.NumKeys) // should be 0
	}
	if err := tree.Insert(11, []byte{0x11}); err != nil {
		t.Fatalf("got error while attempting to insert: %v\n", err)
	}
	if err := tree.Del(1); err == nil { // attempty to delete key that doesn't exist
		t.Fatalf("did not get error while attempting to delete nonexistant key\n")
	}
	if tree.Root.NumKeys != 1 { // check to make sure count doesn't decrement unnecessarily
		t.Fatalf("expected size=1, got: %d\n", tree.Root.NumKeys) // should be 1
	}
	if err := tree.Insert(22, []byte{0x22}); err != nil {
		t.Fatalf("got error while attempting to insert: %v\n", err)
	}
	if tree.Root.NumKeys != 2 { // check to make sure count is correct
		t.Fatalf("expected size=2, got: %d\n", tree.Root.NumKeys) // should be 2
	}
	if err := tree.Insert(33, []byte{0x33}); err != nil { // count=3
		t.Fatalf("got error while attempting to insert: %v\n", err)
	}
	if err := tree.Insert(44, []byte{0x44}); err != nil { // count=4
		t.Fatalf("got error while attempting to insert: %v\n", err)
	}
	if tree.Root.NumKeys != 4 { // check to make sure count is correct
		t.Fatalf("expected size=4, got: %d\n", tree.Root.NumKeys) // should be 4
	}
	if err := tree.Delete(33); err != nil { // del 0x33, now count=3
		t.Fatalf("got error while attempting to delete: %v\n", err)
	}
	if tree.Root.NumKeys != 3 { // check to make sure count doesn't decrement unnecessarily
		t.Fatalf("expected size=3, got: %d\n", tree.Root.NumKeys) // should be 3
	}
	if err := tree.Insert(55, []byte{0x55}); err != nil { // put 0x55, count=4
		t.Fatalf("got error while attempting to insert: %v\n", err)
	}
	if err := tree.Delete(11); err != nil { // del 0x11, count=3
		t.Fatalf("got error while attempting to delete: %v\n", err)
	}
	if tree.Root.NumKeys != 3 { // check to make sure count is correct
		t.Fatalf("expected size=3, got: %d\n", tree.Root.NumKeys)
	}
	if err := tree.Delete(44); err != nil { // del 0x44, count=2
		t.Fatalf("got error while attempting to delete: %v\n", err)
	}
	if err := tree.Delete(22); err != nil { // del 0x22, count=1
		t.Fatalf("got error while attempting to delete: %v\n", err)
	}
	if tree.Root.NumKeys != 1 { // check to make sure count is correct
		t.Fatalf("expected size=1, got: %d\n", tree.Root.NumKeys)
	}
	if err := tree.Delete(55); err != nil { // del 0x55, count=0
		t.Fatalf("got error while attempting to delete: %v\n", err)
	}
	if tree.Root.NumKeys != 0 { // check to make sure count is correct
		t.Fatalf("expected size=0, got: %d\n", tree.Root.NumKeys)
	}
}

// btree set sequential
func Benchmark_BPTree_SetSeq_1e3(b *testing.B) {
	benchmark_BPTree_SetSeq(b, 1e3)
}

func Benchmark_BPTree_SetSeq_1e4(b *testing.B) {
	benchmark_BPTree_SetSeq(b, 1e4)
}

func Benchmark_BPTree_SetSeq_1e5(b *testing.B) {
	benchmark_BPTree_SetSeq(b, 1e5)
}

// func Benchmark_BPTree_SetSeq_1e6(b *testing.B) {
// 	benchmark_BPTree_SetSeq(b, 1e6)
// }

func benchmark_BPTree_SetSeq(b *testing.B, n int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := new(btree)
		debug.FreeOSMemory()
		b.StartTimer()
		for j := 0; j < n; j++ {
			tree.Set(Key([]byte(strconv.Itoa(j))), []byte{0xde, 0xad, 0xbe, 0xef})
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
func Benchmark_BPTree_SetRnd_1e3(b *testing.B) {
	benchmark_BPTree_SetRnd(b, 1e3)
}

func Benchmark_BPTree_SetRnd_1e4(b *testing.B) {
	benchmark_BPTree_SetRnd(b, 1e4)
}

func Benchmark_BPTree_SetRnd_1e5(b *testing.B) {
	benchmark_BPTree_SetRnd(b, 1e5)
}

// func Benchmark_BPTree_SetRnd_1e6(b *testing.B) {
// 	benchmark_BPTree_SetRnd(b, 1e6)
// }

func benchmark_BPTree_SetRnd(b *testing.B, n int) {
	a := rand.New(rand.NewSource(98647)).Perm(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := new(btree)
		debug.FreeOSMemory()
		b.StartTimer()
		for _, v := range a {
			kv := strconv.Itoa(v)
			tree.Set(Key([]byte(kv)), []byte{0xde, 0xad, 0xbe, 0xef})
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
func Benchmark_BPTree_GetSeq_1e3(b *testing.B) {
	benchmark_BPTree_GetSeq(b, 1e3)
}

func Benchmark_BPTree_GetSeq_1e4(b *testing.B) {
	benchmark_BPTree_GetSeq(b, 1e4)
}

func Benchmark_BPTree_GetSeq_1e5(b *testing.B) {
	benchmark_BPTree_GetSeq(b, 1e5)
}

// func Benchmark_BPTree_GetSeq_1e6(b *testing.B) {
// 	benchmark_BPTree_GetSeq(b, 1e6)
// }

func benchmark_BPTree_GetSeq(b *testing.B, n int) {
	tree := new(btree)
	for i := 0; i < n; i++ {
		tree.Set(Key([]byte(strconv.Itoa(i))), []byte{0xde, 0xad, 0xbe, 0xef})
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			kv := strconv.Itoa(j)
			if dat := tree.Get(Key([]byte(kv))); !bytes.Equal(dat, []byte{0xde, 0xad, 0xbe, 0xef}) {
				b.Fatalf("expected %+#v, but got: %+#v\n", kv, dat)
			}
		}
	}
	b.StopTimer()
	tree.Close()
}

// btree get random
func Benchmark_BPTree_GetRnd_1e3(b *testing.B) {
	benchmark_BPTree_GetRnd(b, 1e3)
}

func Benchmark_BPTree_GetRnd_1e4(b *testing.B) {
	benchmark_BPTree_GetRnd(b, 1e4)
}

func Benchmark_BPTree_GetRnd_1e5(b *testing.B) {
	benchmark_BPTree_GetRnd(b, 1e5)
}

// func Benchmark_BPTree_GetRnd_1e6(b *testing.B) {
// 	benchmark_BPTree_GetRnd(b, 1e6)
// }

func benchmark_BPTree_GetRnd(b *testing.B, n int) {
	tree := new(btree)
	a := rand.New(rand.NewSource(59684)).Perm(n)
	for _, v := range a { // fill tree with random data
		tree.Set(Key([]byte(strconv.Itoa(v))), []byte{0xde, 0xad, 0xbe, 0xef})
	}
	debug.FreeOSMemory() // free memory, run gc
	b.ResetTimer()       // and reset timer
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			kv := strconv.Itoa(v)
			if dat := tree.Get(Key([]byte(kv))); !bytes.Equal(dat, []byte{0xde, 0xad, 0xbe, 0xef}) {
				b.Fatalf("expected %+#v, but got: %+#v\n", kv, dat)
			}
		}
	}
	b.StopTimer() // stop the timer and close tree.
	tree.Close()
}

// btree del sequential
func Benchmark_BPTree_DelSeq_1e3(b *testing.B) {
	benchmark_BPTree_DelSeq(b, 1e3)
}

func Benchmark_BPTree_DelSeq_1e4(b *testing.B) {
	benchmark_BPTree_DelSeq(b, 1e4)
}

func Benchmark_BPTree_DelSeq_1e5(b *testing.B) {
	benchmark_BPTree_DelSeq(b, 1e5)
}

// func Benchmark_BPTree_DelSeq_1e6(b *testing.B) {
// 	benchmark_BPTree_DelSeq(b, 1e6)
// }

func benchmark_BPTree_DelSeq(b *testing.B, n int) {
	tree := new(btree)
	for i := 0; i < n; i++ {
		tree.Set(Key([]byte(strconv.Itoa(i))), []byte{0xde, 0xad, 0xbe, 0xef})
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			kv := Key([]byte(strconv.Itoa(j)))
			tree.Del(kv)
			b.StopTimer()
			if tree.Has(kv) {
				b.Fatalf("key %s exists", kv)
			}
			b.StartTimer()
		}
	}
	b.StopTimer()
	tree.Close()
}

// btree get random
func Benchmark_BPTree_DelRnd_1e3(b *testing.B) {
	benchmark_BPTree_DelRnd(b, 1e3)
}

func Benchmark_BPTree_DelRnd_1e4(b *testing.B) {
	benchmark_BPTree_DelRnd(b, 1e4)
}

func Benchmark_BPTree_DelRnd_1e5(b *testing.B) {
	benchmark_BPTree_DelRnd(b, 1e5)
}

// func Benchmark_BPTree_DelRnd_1e6(b *testing.B) {
// 	benchmark_BPTree_DelRnd(b, 1e6)
// }

func benchmark_BPTree_DelRnd(b *testing.B, n int) {
	tree := new(btree)
	a := rand.New(rand.NewSource(65489)).Perm(n)
	for _, v := range a { // fill tree with random data
		tree.Set(Key([]byte(strconv.Itoa(v))), []byte{0xde, 0xad, 0xbe, 0xef})
	}
	debug.FreeOSMemory() // free memory, run gc
	b.ResetTimer()       // and reset timer
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			kv := Key([]byte(strconv.Itoa(v)))
			tree.Del(kv)
			b.StopTimer()
			if tree.Has(kv) {
				b.Fatalf("key %s exists", kv)
			}
			b.StartTimer()
		}
	}
	b.StopTimer() // stop the timer and close tree.
	tree.Close()
}
