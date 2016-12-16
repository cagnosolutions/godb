package main

import (
	"log"
	"strconv"
	"time"

	"github.com/cagnosolutions/godb"
)

const (
	COUNT = 1024
	DEBUG = false
)

// user struct
type User struct {
	Id        int64   `msgpack:"id"`
	Role      string  `msgpack:"role"`
	Email     string  `msgpack:"email,omitempty"`
	Password  string  `msgpack:"password,omitempty"`
	FirstName string  `msgpack:"firstName,omitempty"`
	LastName  string  `msgpack:"lastName,omitempty"`
	Active    bool    `msgpack:"active"`
	Age       int     `msgpack:"age,omitempty"`
	Modified  int64   `msgpack:"modified,omitempty"`
	Billing   Address `msgpack:"billing,omitempty"`
}

// address struct
type Address struct {
	Id     int64  `msgpack:"id"`
	Street string `msgpack:"street,omitempty"`
	City   string `msgpack:"city,omitempty"`
	State  string `msgpack:"state,omitempty"`
	Zip    int    `msgpack:"zip,omitempty"`
}

// simple orm-ish util to create a new user instance
func NewUser(i int) *User {
	n, p := strconv.Itoa(i), strconv.Itoa(i*i)
	return &User{
		Id: int64(i),
		Role: func() string {
			if i%2 == 0 {
				return "ROLE_ADMIN"
			}
			return "ROLE_USER"
		}(),
		Email:     "user-" + n + "-email@example.com",
		Password:  "pass-" + p,
		FirstName: "FirstName-" + n,
		LastName:  "LastName-" + n,
		Active:    i%2 == 0,
		Age:       i,
		Billing:   *NewAddress(i, i*3),
	}
}

// simple orm-ish util to create a new address instance
func NewAddress(i, ii int) *Address {
	n := strconv.Itoa(i)
	return &Address{
		Id:     time.Now().UnixNano(),
		Street: "10" + n + " Somewhere Lane",
		City:   "Awesome City " + n,
		State: func() string {
			if i%2 == 0 {
				return "PA"
			}
			return "CA"
		}(),
		Zip: ii,
	}
}

var (
	usr  *godb.Store
	data []*User
)

func main() {

	/*
	 *	ADD, GET, and DEL users from store
	 *	open/close store between each call
	 */

	add() // add users to store

	get() // get users from store

	del() // del users from store

}

func add() {

	// generate user data
	log.Printf("Generating user data...\n")
	for i := 1; i <= COUNT; i++ {
		data = append(data, NewUser(i))
	}

	opn() // open store

	// add users to store
	log.Printf("Adding users to store...\n")
	for _, u := range data {
		if err := usr.Add(u.Id, u); err != nil {
			panic(err)
		}
	}

	cls() // close store
}

func get() {

	opn() // open store

	// panic if it could not get a user from the store
	for i := 1; i <= COUNT; i++ {
		var dat User
		if err := usr.Get(i, &dat); err != nil {
			log.Printf("Failed to get record %d\n", i)
			panic(err)
		}
	}

	cls() // close store

}

func del() {

	opn() // open store

	// del users from store
	log.Printf("Deleting users from store...\n")
	for _, u := range data {
		if err := usr.Del(u.Id); err != nil {
			panic(err)
		}
	}

	cls() // close store

}

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
