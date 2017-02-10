package main

import (
	"fmt"
	"log"
	"time"

	"github.com/cagnosolutions/godb"
)

const (
	COUNT = 10000
	DEBUG = false
)

func generateData(n int) {
	r := NewRandInt(n)
	// generate user data
	log.Printf("Generating user data...\n")
	for i := 0; i < n; i++ {
		data = append(data, NewUser(r.Get(), 10))
	}
}

func main() {

	/* +--------------------+ //
	// | OPENING COLLECTION | //
	// +--------------------+ */
	log.Printf("Opening collection ")
	col, err := godb.OpenCollection("./db/users")
	if err != nil {
		panic(err)
	}
	log.Printf("(contains %d entries)\n\n", col.Count())
	time.Sleep(time.Duration(1) * time.Second)

	/* +--------------------+ //
	// | DO STUFF / PROCESS | //
	// +--------------------+ */
	add(col)

	/* +------------------+ //
	// | CLOSE COLLECTION | //
	// +------------------+ */
	log.Printf("Closing the store (contains %d entries)\n\n", col.Count())
	time.Sleep(time.Duration(1) * time.Second)
	if err := col.Close(); err != nil {
		panic(err)
	}
}

/*
func opn() {
	// reopen store to get users
	log.Printf("Opening users store ")
	var err error
	usr, err = godb.OpenStore("./users")
	if err != nil {
		panic(err)
	}
	//see how many users are currently in the store
	log.Printf("(contains %d entries)\n\n", usr.Count())
	time.Sleep(time.Duration(1) * time.Second)
}

func cls() {
	// close store; to see if it flushes the data to disk..
	log.Printf("Closing the store (contains %d entries)\n\n", usr.Count())
	time.Sleep(time.Duration(1) * time.Second)
	if err := usr.Close(); err != nil {
		panic(err)
	}
}
*/
func add(c *godb.Collection) {

	generateData(COUNT)

	log.Printf("Adding users to store...\n")
	for i, u := range data {
		if err := c.Add(u.Id, u); err != nil {
			panic(err)
		}
		if (i+1)%10000 == 0 {
			fmt.Println(i + 1)
		}
	}
}

/*
func get() {

	generateData(COUNT)

	opn() // open store

	// panic if it could not get a user from the store
	for i := 0; i < COUNT; i++ {
		var dat User
		if err := usr.Get(r.Get(), &dat); err != nil {
			log.Printf("Failed to get record %d\n", i)
			panic(err)
		}
	}

	cls() // close store

}

func all() {

	opn()

	var users []User

	if err := usr.All(&users); err != nil {
		panic(err)
	}

	fmt.Printf("len(users): %d\n\n", len(users))

	cls()
}

func qry() {

	opn() // open store

	log.Printf("Querying users in store...\n")

	var users []User

	if err := usr.Query("id > 512 ", &users); err != nil {
		panic(err)
	}

	fmt.Printf("\nlen(users): %d\n", len(users))

	for _, u := range users {
		fmt.Printf("\n%+v\n", u)
	}

	cls() // close store

}

func del() {

	generateData(COUNT)

	opn() // open store

	// del users from store
	log.Printf("Deleting users from store...\n")
	for i := 0; i < COUNT; i++ {
		if err := usr.Del(r.Get()); err != nil {
			panic(err)
		}
	}

	cls() // close store

}
*/
