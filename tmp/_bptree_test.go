package godb

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/collinglass/bptree"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 8)
	},
}

var buf []byte

func init() {
	buf = bufPool.Get().([]byte)
	bufPool.Put(buf)
}

func getVal(n int) []byte {
	buf = bufPool.Get().([]byte)
	binary.PutVarint(buf, int64(n))
	return buf
}

func putVal(b []byte) {
	bufPool.Put(b)
}

// count should print count... duh
var tree2 *bptree.Tree

// test has
func Test_BPTree_Has(t *testing.T) {
	tree2 = bptree.NewTree()
	if _, err := tree2.Find(42, false); err == nil {
		t.Fatalf("expexted err, got nil\n")
	}
	// if tree2.Root.NumKeys != 0 {
	// 	t.Fatalf("expected 0, got: %v\n", tree2.Root.NumKeys)
	// }
}

// test add
func Test_BPTree_Add(t *testing.T) {
	tree2 = bptree.NewTree()
	if err := tree2.Insert(42, []byte{0x99}); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
	}
	// if tree2.Root.NumKeys != 1 {
	// 	t.Fatalf("expected 1, got: %d\n", tree2.Root.NumKeys) // should be 1
	// }
	if r, err := tree2.Find(42, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r.Value, []byte{0x99}) {
			t.Fatalf("expected '0x99', got: %s\n", r)
		}
	}
	if err := tree2.Insert(42, []byte{0x77}); err == nil { // overwrite record, should not work
		t.Fatalf("got nil while overwriting\n")
	}
	// if tree2.Root.NumKeys != 1 {
	// 	t.Fatalf("expected 1, got: %d\n", tree2.Root.NumKeys) // should be 1
	// }
	if r, err := tree2.Find(42, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r.Value, []byte{0x99}) {
			t.Fatalf("expected '0x99', got: %s\n", r)
		}
	}
	if err := tree2.Insert(22, []byte{0x44}); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
	}
	// if tree2.Root.NumKeys != 2 {
	// 	t.Fatalf("expected 2, got: %d\n", tree2.Root.NumKeys) // should be 2
	// }
	if r, err := tree2.Find(22, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r.Value, []byte{0x44}) {
			t.Fatalf("expected '0x44', got: %s\n", r)
		}
	}
}

// test get
func Test_BPTree_Get(t *testing.T) {
	tree2 = bptree.NewTree()
	// if tree2.Root.NumKeys != 0 {
	// 	t.Fatalf("expected 0, got: %d\n", tree2.Root.NumKeys)
	// }
	if r, err := tree2.Find(11, false); err == nil {
		t.Fatalf("expected nil, got: %s\n", r.Value)
	}
	if err := tree2.Insert(11, []byte{0x01}); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
	}
	// if tree2.Root.NumKeys != 1 {
	// 	t.Fatalf("expected 1, got: %d\n", tree2.Root.NumKeys)
	// }
	if r, err := tree2.Find(11, false); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
		if !bytes.Equal(r.Value, []byte{0x01}) {
			t.Fatalf("expected '0x01', got: %s\n", r.Value)
		}
	}
}

// test set
func Test_BPTree_Set(t *testing.T) {
	tree2 = bptree.NewTree()
	if err := tree2.Insert(42, []byte{0x99}); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
	}
	// if tree2.Root.NumKeys != 1 {
	// 	t.Fatalf("expected 1, got: %d\n", tree2.Root.NumKeys) // should be 1
	// }
	if r, err := tree2.Find(42, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r.Value, []byte{0x99}) {
			t.Fatalf("expected '0x99', got: %s\n", r)
		}
	}
	if err := tree2.Insert(42, []byte{0x77}); err == nil { // overwrite record
		t.Fatalf("got nil while overwriting\n")
	}
	// if tree2.Root.NumKeys != 1 {
	// 	t.Fatalf("expected 1, got: %d\n", tree2.Root.NumKeys) // should be 1
	// }
	if r, err := tree2.Find(42, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r.Value, []byte{0x77}) {
			t.Fatalf("expected '0x77', got: %s\n", r)
		}
	}
	if err := tree2.Insert(22, []byte{0x44}); err != nil {
		t.Fatalf("got error while inserting: %v\n", err)
	}
	// if tree2.Root.NumKeys != 2 {
	// 	t.Fatalf("expected 2, got: %d\n", tree2.Root.NumKeys) // should be 2
	// }
	if r, err := tree2.Find(22, false); err != nil {
		t.Fatalf("got error while finding: %v\n", err)
		if !bytes.Equal(r.Value, []byte{0x44}) {
			t.Fatalf("expected '0x44', got: %s\n", r)
		}
	}
}

// test del
func Test_BPTree_Del(t *testing.T) {
	tree2 = bptree.NewTree()
	//if err := tree2.Delete(11); err == nil { // delete nonexistant key
	//	t.Fatalf("did not get error while attempting to deleting nonexistant key\n")
	//}
	// if tree2.Root.NumKeys != 0 { // check to make sure count doesn't decrement unnecessarily
	// 	t.Fatalf("expected size=0, got: %d\n", tree2.Root.NumKeys) // should be 0
	// }
	if err := tree2.Insert(11, []byte{0x11}); err != nil {
		t.Fatalf("got error while attempting to insert: %v\n", err)
	}
	//if err := tree2.Delete(1); err == nil { // attempty to delete key that doesn't exist
	//	t.Fatalf("did not get error while attempting to delete nonexistant key\n")
	//}
	// if tree2.Root.NumKeys != 1 { // check to make sure count doesn't decrement unnecessarily
	// 	t.Fatalf("expected size=1, got: %d\n", tree2.Root.NumKeys) // should be 1
	// }
	if err := tree2.Insert(22, []byte{0x22}); err != nil {
		t.Fatalf("got error while attempting to insert: %v\n", err)
	}
	// if tree2.Root.NumKeys != 2 { // check to make sure count is correct
	// 	t.Fatalf("expected size=2, got: %d\n", tree2.Root.NumKeys) // should be 2
	// }
	if err := tree2.Insert(33, []byte{0x33}); err != nil { // count=3
		t.Fatalf("got error while attempting to insert: %v\n", err)
	}
	if err := tree2.Insert(44, []byte{0x44}); err != nil { // count=4
		t.Fatalf("got error while attempting to insert: %v\n", err)
	}
	// if tree2.Root.NumKeys != 4 { // check to make sure count is correct
	// 	t.Fatalf("expected size=4, got: %d\n", tree2.Root.NumKeys) // should be 4
	// }
	if err := tree2.Delete(33); err != nil { // del 0x33, now count=3
		t.Fatalf("got error while attempting to delete: %v\n", err)
	}
	// if tree2.Root.NumKeys != 3 { // check to make sure count doesn't decrement unnecessarily
	// 	t.Fatalf("expected size=3, got: %d\n", tree2.Root.NumKeys) // should be 3
	// }
	if err := tree2.Insert(55, []byte{0x55}); err != nil { // put 0x55, count=4
		t.Fatalf("got error while attempting to insert: %v\n", err)
	}
	if err := tree2.Delete(11); err != nil { // del 0x11, count=3
		t.Fatalf("got error while attempting to delete: %v\n", err)
	}
	// if tree2.Root.NumKeys != 3 { // check to make sure count is correct
	// 	t.Fatalf("expected size=3, got: %d\n", tree2.Root.NumKeys)
	// }
	if err := tree2.Delete(44); err != nil { // del 0x44, count=2
		t.Fatalf("got error while attempting to delete: %v\n", err)
	}
	if err := tree2.Delete(22); err != nil { // del 0x22, count=1
		t.Fatalf("got error while attempting to delete: %v\n", err)
	}
	// if tree2.Root.NumKeys != 1 { // check to make sure count is correct
	// 	t.Fatalf("expected size=1, got: %d\n", tree2.Root.NumKeys)
	// }
	if err := tree2.Delete(55); err != nil { // del 0x55, count=0
		t.Fatalf("got error while attempting to delete: %v\n", err)
	}
	// if tree2.Root.NumKeys != 0 { // check to make sure count is correct
	// 	t.Fatalf("expected size=0, got: %d\n", tree2.Root.NumKeys)
	// }
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
		tree2 := bptree.NewTree()
		debug.FreeOSMemory()
		b.StartTimer()
		for j := 0; j < n; j++ {
			buf := getVal(j)
			if err := tree2.Insert(j, buf); err != nil {
				b.Fatalf("got error while inserting: %v\n", err)
			}
			putVal(buf)
		}
		b.StopTimer()
		// if tree2.Root.NumKeys != n {
		// 	b.Fatalf("expected %d entries, got: %d entries instead\n", n, tree2.Root.NumKeys)
		// }
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
		tree2 := bptree.NewTree()
		debug.FreeOSMemory()
		b.StartTimer()
		for _, v := range a {
			buf := getVal(v)
			if err := tree2.Insert(v, buf); err != nil {
				b.Fatalf("got error while inserting: %v\n", err)
			}
			putVal(buf)
		}
		b.StopTimer()
		// if tree2.Root.NumKeys != n {
		// 	b.Fatalf("expected %d entries, got: %d entries instead\n", n, tree2.Root.NumKeys)
		// }
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
	tree2 := bptree.NewTree()
	for i := 0; i < n; i++ {
		buf := getVal(i)
		if err := tree2.Insert(i, buf); err != nil {
			b.Fatalf("got error while inserting: %v\n", err)
		}
		putVal(buf)
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			r, err := tree2.Find(j, false)
			if err != nil {
				b.Fatalf("got error while finding: %v\n", err)
			}
			buf := getVal(j)
			if !bytes.Equal(r.Value, buf) {
				b.Fatalf("expected %+#v, but got: %+#v\n", buf, r.Value)
			}
			putVal(buf)
		}
	}
	b.StopTimer()
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
	tree2 := bptree.NewTree()
	a := rand.New(rand.NewSource(59684)).Perm(n)
	for _, v := range a { // fill tree with random data
		buf := getVal(v)
		if err := tree2.Insert(v, buf); err != nil {
			b.Fatalf("got error while inserting: %v\n", err)
		}
		putVal(buf)
	}
	debug.FreeOSMemory() // free memory, run gc
	b.ResetTimer()       // and reset timer
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			if r, err := tree2.Find(v, false); err != nil {
				buf := getVal(v)
				if !bytes.Equal(r.Value, buf) {
					b.Fatalf("expected %+#v, but got: %+#v\n", buf, r.Value)
				}
				putVal(buf)
			}
		}
	}
	b.StopTimer() // stop the timer and close tree2.
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
	tree2 := bptree.NewTree()
	for i := 0; i < n; i++ {
		buf := getVal(i)
		if err := tree2.Insert(i, buf); err != nil {
			b.Fatalf("got error while inserting: %v\n", err)
		}
		putVal(buf)
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			if err := tree2.Delete(j); err != nil {
				b.Fatalf("got err while deleteing; %v\n", err)
			}
			//b.StopTimer()
			//if _, err := tree2.Find(j, false); err == nil {
			//	b.Fatalf("key %d exists, bufue: %s\n", j)
			//boot}
			//b.StartTimer()
		}
	}
	b.StopTimer()
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
	tree2 := bptree.NewTree()
	a := rand.New(rand.NewSource(65489)).Perm(n)
	for _, v := range a { // fill tree with random data
		buf := getVal(v)
		if err := tree2.Insert(v, buf); err != nil {
			b.Fatalf("got error while inserting: %v\n", err)
		}
		putVal(buf)
	}
	debug.FreeOSMemory() // free memory, run gc
	b.ResetTimer()       // and reset timer
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			if err := tree2.Delete(v); err != nil {
				b.Fatalf("got error while deleting: %v\n", err)
			}
			//b.StopTimer()
			//if _, err := tree2.Find(v, false); err == nil {
			//	b.Fatalf("key %d exists", v)
			//}
			//b.StartTimer()
		}
	}
	b.StopTimer() // stop the timer and close tree2.
}
