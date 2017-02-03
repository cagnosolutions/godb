package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/cagnosolutions/godb"
)

const (
	COUNT = 10000
	DEBUG = false
)

// user struct
type User struct {
	Id        int64      `msgpack:"id"`
	Role      string     `msgpack:"role"`
	Email     string     `msgpack:"email,omitempty"`
	Password  string     `msgpack:"password,omitempty"`
	FirstName string     `msgpack:"firstName,omitempty"`
	LastName  string     `msgpack:"lastName,omitempty"`
	Active    bool       `msgpack:"active"`
	Age       int        `msgpack:"age,omitempty"`
	Modified  int64      `msgpack:"modified,omitempty"`
	Addresses []*Address `msgpack:"addresses,omitempty"`
	Jobs      []*Job     `msgpack:"jobs"`
}

type addrType byte

const (
	BILL addrType = iota
	SHIP
)

// address struct
type Address struct {
	Id     int64    `msgpack:"id"`
	Type   addrType `msgpack:"type"`
	Street string   `msgpack:"street,omitempty"`
	City   string   `msgpack:"city,omitempty"`
	State  string   `msgpack:"state,omitempty"`
	Zip    int      `msgpack:"zip,omitempty"`
}

// job struct
type Job struct {
	Id        int64       `msgpack:"id"`
	Name      string      `msgpack:"name"`
	Materials []*Material `msgpack:"materials"`
	Total     float32     `msgpack:"total"`
}

func NewJob(i int) *Job {
	n := strconv.Itoa(i)
	j := &Job{
		Id:   int64(i),
		Name: "new job #" + n,
	}
	for i := 0; i < 5; i++ {
		j.Materials = append(j.Materials, NewMaterial(i))
		j.Total += j.Materials[i].GetPrice()
	}
	return j
}

// material struct
type Material struct {
	Id    int64   `msgpack:"id"`
	Name  string  `msgpack:"name"`
	Desc  string  `msgpack:"description"`
	Cost  float32 `msgpack:"cost"`
	Price float32 `msgpack:"price"`
}

func (m *Material) GetPrice() float32 {
	return m.Price
}

func NewMaterial(i int) *Material {
	n := strconv.Itoa(i)
	return &Material{
		Id:    int64(i),
		Name:  "material-" + n,
		Desc:  "a lengthy description of 'material #" + n + "'",
		Cost:  float32(i) + float32(i+1/36),
		Price: float32(i) + float32(i+1/25),
	}
}

// simple orm-ish util to create a new user instance
func NewUser(i int, ii int) *User {
	n, p := strconv.Itoa(i), strconv.Itoa(i*i)
	u := &User{
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
	}
	for j := 0; j < ii; j++ {
		u.Addresses = append(u.Addresses, NewAddress(i, i*3))
		u.Jobs = append(u.Jobs, NewJob(i))
		if !u.Active {
			break
		}
	}
	return u
}

// simple orm-ish util to create a new address instance
func NewAddress(i, ii int) *Address {
	n := strconv.Itoa(i)
	return &Address{
		Id: time.Now().UnixNano(),
		Type: func() addrType {
			if i%2 == 0 {
				return BILL
			}
			return SHIP
		}(),
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

	// get() // get users from store

	// qry()

	// all()

	//del() // del users from store

	/*opn()
	fmt.Println("sleeping 5 seconds...")
	time.Sleep(5 * time.Second)
	cls()*/

}

func add() {
	r := NewRandInt(COUNT)
	// generate user data
	log.Printf("Generating user data...\n")
	for i := 0; i < COUNT; i++ {
		data = append(data, NewUser(r.Get(), 30))
	}

	opn() // open store

	// add users to store
	log.Printf("Adding users to store...\n")
	for i, u := range data {
		if err := usr.Add(u.Id, u); err != nil {
			panic(err)
		}
		if (i+1)%10000 == 0 {
			fmt.Println(i + 1)
		}
	}
	cls() // close store
}

func get() {

	r := NewRandInt(COUNT)

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
	r := NewRandInt(COUNT)
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

type RandInt struct {
	m map[int]bool
	n int
}

func NewRandInt(n int) *RandInt {
	return &RandInt{
		m: make(map[int]bool, 0),
		n: n,
	}
}

func (r *RandInt) Get() int {
	var n int
	for {
		n = rand.Intn(r.n)
		if !r.m[n] {
			r.m[n] = true
			break
		} else if len(r.m) == r.n {
			panic("exceeded maximum size")
		}
	}
	return n
}

func genId() int64 {
	return time.Now().UnixNano()
}
