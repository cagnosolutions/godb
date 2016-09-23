package godb

import (
	"runtime/debug"
	"strconv"
	"testing"
)

func Benchmark_NewK_1e1(b *testing.B) {
	benchmark_NewK_N(b, 1e1)
}

func Benchmark_NewK_1e2(b *testing.B) {
	benchmark_NewK_N(b, 1e2)
}

func Benchmark_NewK_1e3(b *testing.B) {
	benchmark_NewK_N(b, 1e3)
}

func Benchmark_NewK_1e4(b *testing.B) {
	benchmark_NewK_N(b, 1e4)
}

func Benchmark_NewK_1e5(b *testing.B) {
	benchmark_NewK_N(b, 1e5)
}

func Benchmark_NewK_1e6(b *testing.B) {
	benchmark_NewK_N(b, 1e6)
}

func benchmark_NewK_N(b *testing.B, n int) {
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			b.StopTimer()
			kv := []byte("key-n-" + strconv.Itoa(j))
			b.StartTimer()
			if d := key(kv); d[0] == 0x00 {
				b.Fatalf(string(d[:]))
			}
		}
	}
	b.StopTimer()
}
