package main

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/cagnosolutions/godb"
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
	usr  *godb.Collection
	data []*User
)

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
