package godb

import (
	"fmt"
	"testing"
)

// count should print count... duh
var tmap *STMap
var tmd = func(s string, args ...interface{}) string {
	return fmt.Sprintf(s, args...)
}

func Benchmark_STMap_Has(b *testing.B) {
	b.StopTimer()
	tmap = NewSTMap()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		d := tmd("data-%.3d", i)
		if tmap.Has(d) {
			// should be empty tmap
			b.FailNow()
		}
	}
	b.StopTimer()
	tmap.Close()
}

func Benchmark_STMap_Add(b *testing.B) {
	b.StopTimer()
	tmap = NewSTMap()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		d := tmd("data-%.3d", i)
		tmap.Add(d, []byte(d))
	}
	b.StopTimer()
	tmap.Close()
}

func Benchmark_STMap_Set(b *testing.B) {
	b.StopTimer()
	tmap = NewSTMap()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		d := tmd("data-%.3d", i)
		tmap.Set(d, []byte(d))
	}
	b.StopTimer()
	tmap.Close()
}

func Benchmark_STMap_Get(b *testing.B) {
	tmap = NewSTMap()
	for i := 0; i < b.N; i++ {
		d := tmd("data-%.3d", i)
		tmap.Set(d, []byte(d))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := tmd("data-%.3d", i)
		if tmap.Get(d) == nil {
			b.FailNow()
		}
	}
	b.StopTimer()
	tmap.Close()
}

func Benchmark_STMap_Del(b *testing.B) {
	tmap = NewSTMap()
	for i := 0; i < b.N; i++ {
		d := tmd("data-%.3d", i)
		tmap.Set(d, []byte(d))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := tmd("data-%.3d", i)
		tmap.Del(d)
	}
	b.StopTimer()
	tmap.Close()
}
