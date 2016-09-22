package main

import (
	"fmt"
	"math/rand"

	"github.com/cagnosolutions/godb"
)

const COUNT = 64

func gen(str string, args ...interface{}) []byte {
	return []byte(fmt.Sprintf(str, args...))
}

func main() {

	t := godb.NewBTree()

	a := func() map[int]struct{} {
		n := make(map[int]struct{}, 0)
		for i := 0; i < COUNT; i++ {
			n[rand.Intn(COUNT)] = struct{}{}
			//n = append(n, rand.Intn(COUNT))
		}
		return n
	}()

	fmt.Printf("\nKeys Generated: %d\n\n", len(a))

	for c, _ := range a {
		k, v := gen("key-%.2d", c), gen("val-%.2d", c)
		fmt.Printf("inserting key: %s, val: %s\n", k, v)
		t.Set(k, v)
	}

	fmt.Printf("\nTree contains %d entries...\n\n", t.Count())

	for c, _ := range a {
		k := gen("key-%.2d", c)
		v := t.Get(k)
		fmt.Printf("got val: %s, for key: %s\n", v, k)
	}

	fmt.Println()

	t.Print()

	fmt.Println("Deleting entries\n")

	for c, _ := range a {
		k := gen("key-%.2d", c)
		t.Del(k)
		fmt.Printf("deleting key: %s\n", k)
		t.Print()
	}

	// t.Close()

	fmt.Println()

	//fmt.Println(t.PrintJSON())

}
