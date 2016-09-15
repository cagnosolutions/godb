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
		n := gen("key-val-%.3d", c)
		fmt.Printf("inserting key: %s\n", n)
		t.Set(n, n)
	}

	fmt.Printf("\nTree contains %d entries...\n\n", t.Count())

	for c, _ := range a {
		n := gen("key-val-%.3d", c)
		x := t.Get(n)
		fmt.Printf("got val: %s\n", x)
	}

	fmt.Println()

	t.Print()

	// fmt.Println("Deleting entries\n")
	//
	// for c, _ := range a {
	// 	n := gen("key-val-%.3d", c)
	// 	t.Del(n)
	// 	fmt.Printf("deleted key: %s\n\n", n)
	// 	t.Print()
	// }
	//
	// t.Close()

	fmt.Println()

	//fmt.Println(t.PrintJSON())

}
