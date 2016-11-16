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
	Id        int64   `json:"id"`
	Role      string  `json:"role"`
	Email     string  `json:"email,omitempty"`
	Password  string  `json:"password,omitempty"`
	FirstName string  `json:"firstName,omitempty"`
	LastName  string  `json:"lastName,omitempty"`
	Active    bool    `json:"active"`
	Age       int     `json:"age,omitempty"`
	Modified  int64   `json:"modified,omitempty"`
	Billing   Address `json:"billing,omitempty"`
}

// address struct
type Address struct {
	Id     int64  `json:"id"`
	Street string `json:"street,omitempty"`
	City   string `json:"city,omitempty"`
	State  string `json:"state,omitempty"`
	Zip    int    `json:"zip,omitempty"`
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

func main() {

	/*// generate user data
	log.Printf("Generating user data...\n")
	var data []*User
	for i := 1; i <= COUNT; i++ {
		data = append(data, NewUser(i))
	}

	// open a users store
	log.Printf("Opening users store...\n")
	usr, err := godb.OpenStore("./users")
	if err != nil {
		panic(err)
	}

	// add users to store
	log.Printf("Adding users to store...\n")
	for _, u := range data {
		if err := usr.Add(u.Id, u); err != nil {
			panic(err)
		}
	}*/

	// see how many users are currently in the store
	// log.Printf("Store currently contains %d entries...\n\n", usr.Count())

	// close store; to see if it flushes the data to disk..
	//if err := usr.Close(); err != nil {
	//	panic(err)
	//}

	//return

	// reopen store to get users
	log.Printf("Opening users store...\n")
	usr, err := godb.OpenStore("./users")
	if err != nil {
		panic(err)
	}

	for i := 1; i <= COUNT; i++ {
		var dat User
		if err := usr.Get(i, &dat); err != nil {
			log.Printf("Failed to get record %d\n", i)
			panic(err)
		}
	}

	// get users from store
	/*log.Printf("Getting users from store...\n")
	for _, u := range data {
		var dat User
		if err := usr.Get(u.Id, &dat); err != nil {
			panic(err)
		}
	}*/

	// del users from store
	//log.Printf("Deleting users from store...\n")
	//for _, u := range data {
	//	if err := usr.Del(u.Id); err != nil {
	//		panic(err)
	//	}
	//}

	// see how many users are currently in the store
	log.Printf("Store currently contains %d entries...\n\n", usr.Count())

	// closing user store
	log.Printf("Closing user store.")
	if err := usr.Close(); err != nil {
		panic(err)
	}

}
