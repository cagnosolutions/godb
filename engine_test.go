package godb

import (
	"bytes"
	"math/rand"
	"runtime/debug"
	"strconv"
	"testing"
)

// count should print count... duh
var tree *btree

// test has
func Test_BTree_Has(t *testing.T) {
	tree = new(btree)
	if ok := tree.has([]byte{0x42}); ok {
		t.Fatalf("expexted nil, got: %v\n", ok)
	}
	if tree.count != 0 {
		t.Fatalf("expected 0, got: %v\n", tree.count)
	}
}

// test add
func Test_BTree_Add(t *testing.T) {
	tree := new(btree)
	tree.add([]byte{0x42}, []byte{0x99})
	if tree.count != 1 {
		t.Fatalf("expected 1, got: %d\n", tree.count) // should be 1
	}
	if dat := tree.get([]byte{0x42}); !bytes.Equal(dat, []byte{0x99}) {
		t.Fatalf("expected '0x99', got: %s\n", dat)
	}
	tree.add([]byte{0x42}, []byte{0x77}) // overwrite record, should no work
	if tree.count != 1 {
		t.Fatalf("expected 1, got: %d\n", tree.count) // should be 1
	}
	if dat := tree.get([]byte{0x42}); !bytes.Equal(dat, []byte{0x99}) {
		t.Fatalf("expected '0x99', got: %s\n", dat)
	}
	tree.add([]byte{0x22}, []byte{0x44})
	if tree.count != 2 {
		t.Fatalf("expected 2, got: %d\n", tree.count) // should be 2
	}
	if dat := tree.get([]byte{0x22}); !bytes.Equal(dat, []byte{0x44}) {
		t.Fatalf("expected '0x44', got: %s\n", dat)
	}
}

// test get
func Test_BTree_Get(t *testing.T) {
	tree = new(btree)
	if tree.count != 0 {
		t.Fatalf("expected 0, got: %d\n", tree.count)
	}
	if dat := tree.get([]byte{0x11}); dat != nil {
		t.Fatalf("expexted nil, got: %s\n", dat)
	}
	tree.set([]byte{0x11}, []byte{0x01})
	if tree.count != 1 {
		t.Fatalf("expected 1, got: %d\n", tree.count)
	}
	if dat := tree.get([]byte{0x11}); !bytes.Equal(dat, []byte{0x01}) {
		t.Fatalf("expected '0x01', got: %s\n", dat)
	}
}

// test set
func Test_BTree_Set(t *testing.T) {
	tree := new(btree)
	tree.set([]byte{0x42}, []byte{0x99})
	if tree.count != 1 {
		t.Fatalf("expected 1, got: %d\n", tree.count) // should be 1
	}
	if dat := tree.get([]byte{0x42}); !bytes.Equal(dat, []byte{0x99}) {
		t.Fatalf("expected '0x99', got: %s\n", dat)
	}
	tree.set([]byte{0x42}, []byte{0x77}) // overwrite record
	if tree.count != 1 {
		t.Fatalf("expected 1, got: %d\n", tree.count) // should be 1
	}
	if dat := tree.get([]byte{0x42}); !bytes.Equal(dat, []byte{0x77}) {
		t.Fatalf("expected '0x77', got: %s\n", dat)
	}
	tree.set([]byte{0x22}, []byte{0x44})
	if tree.count != 2 {
		t.Fatalf("expected 2, got: %d\n", tree.count) // should be 2
	}
	if dat := tree.get([]byte{0x22}); !bytes.Equal(dat, []byte{0x44}) {
		t.Fatalf("expected '0x44', got: %s\n", dat)
	}
}

// test del
func Test_BTree_Del(t *testing.T) {
	tree := new(btree)
	tree.del([]byte{0x11}) // delete non-existant key
	if tree.count != 0 {   // check to make sure count doesn't decrement unnecessarily
		t.Fatalf("expected size=0, got: %d\n", tree.count) // should be 0
	}
	tree.set([]byte{0x11}, []byte{0x11}) // set key
	tree.del([]byte{0x01})               // attempty to delete key that doesn't exist
	if tree.count != 1 {                 // check to make sure count doesn't decrement unnecessarily
		t.Fatalf("expected size=1, got: %d\n", tree.count) // should be 1
	}
	tree.set([]byte{0x22}, []byte{0x22})
	if tree.count != 2 { // check to make sure count is correct
		t.Fatalf("expected size=2, got: %d\n", tree.count) // should be 2
	}
	tree.set([]byte{0x33}, []byte{0x33}) // count=3
	tree.set([]byte{0x44}, []byte{0x44}) // count=4
	if tree.count != 4 {                 // check to make sure count is correct
		t.Fatalf("expected size=4, got: %d\n", tree.count) // should be 4
	}
	tree.del([]byte{0x33}) // del 0x33, now count=3
	if tree.count != 3 {   // check to make sure count doesn't decrement unnecessarily
		t.Fatalf("expected size=3, got: %d\n", tree.count) // should be 3
	}
	tree.set([]byte{0x55}, []byte{0x55}) // put 0x55, count=4
	tree.del([]byte{0x11})               // del 0x11, count=3
	if tree.count != 3 {                 // check to make sure count is correct
		t.Fatalf("expected size=3, got: %d\n", tree.count)
	}
	tree.del([]byte{0x44}) // del 0x44, count=2
	tree.del([]byte{0x22}) // del 0x22, count=1
	if tree.count != 1 {   // check to make sure count is correct
		t.Fatalf("expected size=1, got: %d\n", tree.count)
	}
	tree.del([]byte{0x55}) // del 0x55, count=0

	if tree.count != 0 { // check to make sure count is correct
		t.Fatalf("expected size=0, got: %d\n", tree.count)
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

// func Benchmark_BTree_SetSeq_1e6(b *testing.B) {
// 	benchmark_BTree_SetSeq(b, 1e6)
// }

func benchmark_BTree_SetSeq(b *testing.B, n int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := new(btree)
		debug.FreeOSMemory()
		b.StartTimer()
		for j := 0; j < n; j++ {
			tree.set([]byte(strconv.Itoa(j)), []byte{0xde, 0xad, 0xbe, 0xef})
		}
		b.StopTimer()
		if tree.count != n {
			b.Fatalf("expected %d entries, got: %d entries instead\n", n, tree.count)
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

// func Benchmark_BTree_SetRnd_1e6(b *testing.B) {
// 	benchmark_BTree_SetRnd(b, 1e6)
// }

func benchmark_BTree_SetRnd(b *testing.B, n int) {
	a := rand.New(rand.NewSource(98647)).Perm(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := new(btree)
		debug.FreeOSMemory()
		b.StartTimer()
		for _, v := range a {
			kv := strconv.Itoa(v)
			tree.set([]byte(kv), []byte{0xde, 0xad, 0xbe, 0xef})
		}
		b.StopTimer()
		if tree.count != n {
			b.Fatalf("expected %d entries, got: %d entries instead\n", n, tree.count)
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

// func Benchmark_BTree_GetSeq_1e6(b *testing.B) {
// 	benchmark_BTree_GetSeq(b, 1e6)
// }

func benchmark_BTree_GetSeq(b *testing.B, n int) {
	tree := new(btree)
	for i := 0; i < n; i++ {
		tree.set([]byte(strconv.Itoa(i)), []byte{0xde, 0xad, 0xbe, 0xef})
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			kv := strconv.Itoa(j)
			if dat := tree.get([]byte(kv)); !bytes.Equal(dat, []byte{0xde, 0xad, 0xbe, 0xef}) {
				b.Fatalf("expected %+#v, but got: %+#v\n", kv, dat)
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

// func Benchmark_BTree_GetRnd_1e6(b *testing.B) {
// 	benchmark_BTree_GetRnd(b, 1e6)
// }

func benchmark_BTree_GetRnd(b *testing.B, n int) {
	tree := new(btree)
	a := rand.New(rand.NewSource(59684)).Perm(n)
	for _, v := range a { // fill tree with random data
		tree.set([]byte(strconv.Itoa(v)), []byte{0xde, 0xad, 0xbe, 0xef})
	}
	debug.FreeOSMemory() // free memory, run gc
	b.ResetTimer()       // and reset timer
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			kv := strconv.Itoa(v)
			if dat := tree.get([]byte(kv)); !bytes.Equal(dat, []byte{0xde, 0xad, 0xbe, 0xef}) {
				b.Fatalf("expected %+#v, but got: %+#v\n", kv, dat)
			}
		}
	}
	b.StopTimer() // stop the timer and close tree.
	tree.Close()
}

// btree del sequential
func Benchmark_BTree_DelSeq_1e3(b *testing.B) {
	benchmark_BTree_DelSeq(b, 1e3)
}

func Benchmark_BTree_DelSeq_1e4(b *testing.B) {
	benchmark_BTree_DelSeq(b, 1e4)
}

func Benchmark_BTree_DelSeq_1e5(b *testing.B) {
	benchmark_BTree_DelSeq(b, 1e5)
}

// func Benchmark_BTree_DelSeq_1e6(b *testing.B) {
// 	benchmark_BTree_DelSeq(b, 1e6)
// }

func benchmark_BTree_DelSeq(b *testing.B, n int) {
	tree := new(btree)
	for i := 0; i < n; i++ {
		tree.set([]byte(strconv.Itoa(i)), []byte{0xde, 0xad, 0xbe, 0xef})
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			kv := []byte(strconv.Itoa(j))
			tree.del(kv)
			b.StopTimer()
			if tree.has(kv) {
				b.Fatalf("key %s exists", kv)
			}
			b.StartTimer()
		}
	}
	b.StopTimer()
	tree.Close()
}

// btree get random
func Benchmark_BTree_DelRnd_1e3(b *testing.B) {
	benchmark_BTree_DelRnd(b, 1e3)
}

func Benchmark_BTree_DelRnd_1e4(b *testing.B) {
	benchmark_BTree_DelRnd(b, 1e4)
}

func Benchmark_BTree_DelRnd_1e5(b *testing.B) {
	benchmark_BTree_DelRnd(b, 1e5)
}

// func Benchmark_BTree_DelRnd_1e6(b *testing.B) {
// 	benchmark_BTree_DelRnd(b, 1e6)
// }

func benchmark_BTree_DelRnd(b *testing.B, n int) {
	tree := new(btree)
	a := rand.New(rand.NewSource(65489)).Perm(n)
	for _, v := range a { // fill tree with random data
		tree.set([]byte(strconv.Itoa(v)), []byte{0xde, 0xad, 0xbe, 0xef})
	}
	debug.FreeOSMemory() // free memory, run gc
	b.ResetTimer()       // and reset timer
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			kv := []byte(strconv.Itoa(v))
			tree.del(kv)
			b.StopTimer()
			if tree.has(kv) {
				b.Fatalf("key %s exists", kv)
			}
			b.StartTimer()
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
		if tree.has(d) {
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
		tree.add(d, d)
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
		tree.set(d, d)
	}
	b.StopTimer()
	tree.Close()
}

func Benchmark_BTree_Get(b *testing.B) {
	tree = new(btree)
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		tree.set(d, d)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		if tree.get(d) == nil {
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
		tree.set(d, d)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := data("data-%.3d", i)
		tree.del(d)
	}
	b.StopTimer()
	tree.Close()
}
*/
