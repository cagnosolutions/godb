package msgpack

import (
	"fmt"
	"strconv"
	"strings"
)

func (q *queryResult) assert(v interface{}) bool {
	if q.op == "" {
		return true
	}
	s1, s2 := fmt.Sprintf("%020v", v), fmt.Sprintf("%020v", q.cmp)
	switch q.op {
	case "==":
		return s1 == s2
	case "!=":
		return s1 != s2
	case "<":
		return s1 < s2
	case ">":
		return s1 > s2
	case ">=":
		return s1 >= s2
	case "<=":
		return s1 <= s2
	default:
		return false
	}
}

type queryResult struct {
	key   string
	query string
	op    string
	cmp   string
	match bool
	iter  int
}

// assign the next key by locating the index of the dot seperator,
// and simultaneously update the remaining query string.
func (q *queryResult) nextKey() {
	// search for dot
	if ind := strings.IndexByte(q.query, '.'); ind != -1 {
		// update the key, and the query strings respectively
		q.key = q.query[:ind]
		q.query = q.query[ind+1:]
		return
	}
	// we found no dot notation (ie. we have found the "last" key)
	q.key = q.query
	q.query = ""
}

// extracts data specified by the query from the msgpack stream, skipping any other data
func (d *Decoder) Query(q string) (bool, error) {

	// extract the query fields from within the query
	qry := strings.Fields(q)
	if len(qry) != 3 && len(qry) != 1 {
		return false, fmt.Errorf("[msgpack]: invalid number of aruguments or format\n")
	}

	var res queryResult

	if len(qry) == 1 {
		res.query = qry[0]
		goto mark
	}

	// check for valid operator
	if qry[1] != "==" && qry[1] != "!=" && qry[1] != "<" && qry[1] != ">" && qry[1] != ">=" && qry[1] != "<=" {
		return false, fmt.Errorf("[msgpack]: invalid operator (%q) supplied, only accepts `==, !=, <, >, >=, <=`\n", qry[1])
	}

	// assemble a query result struct
	// based on the qualified qry string
	res = queryResult{
		query: qry[0],
		op:    qry[1],
		cmp:   qry[2],
	}

mark:
	// pass query result pointer into the internal
	// query method. it will keep it's own state
	// as it recursively parses the query string
	if err := d.query(&res); err != nil {
		return false, err
	}
	// return all matching values
	return res.match, nil
}

func (d *Decoder) query(q *queryResult) error {
	// consume and process the next key in the query
	q.nextKey()

	// we are done processing the query key, so lets
	// assume we have found a matching value and de-
	// code it. if there is no error, we have a match,
	// so lets add it to the matching values list.
	if q.key == "" {
		v, err := d.DecodeInterface()
		if err != nil {
			return err
		}
		if q.assert(v) {
			q.match = true
		}
		return nil
	}

	// code is msgpack type code
	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	switch {
	case q.match == true:
		return nil
	case code == Map16 || code == Map32 || IsFixedMap(code):
		err = d.queryMapKey(q)
	case code == Array16 || code == Array32 || IsFixedArray(code):
		err = d.queryArrayIndex(q)
	default:
		err = fmt.Errorf("[msgpack error] code: \"%v\", key: %q, query: %q\n", code, q.key, q.query)
	}
	return err
}

func (d *Decoder) queryMapKey(q *queryResult) error {
	// check the length of the map
	n, err := d.DecodeMapLen()
	if err != nil {
		return err
	}
	if n == -1 {
		return nil
	}

	// loop
	for i := 0; i < n; i++ {
		// iterate the keys in the map in order
		// to try and find a matching one
		k, err := d.bytesNoCopy()
		if err != nil {
			return err
		}

		// found a matching key
		if string(k) == q.key {
			if err := d.query(q); err != nil {
				return err
			}
			if q.iter > 0 {
				// skip to the next type (array, map, etc.) in outer structure
				return d.skipNext((n - i - 1) * 2)
			}
			return nil
		}

		// move (the cursor) to the next key key/value set in the map
		if err := d.Skip(); err != nil {
			return err
		}
	}

	return nil
}

func (d *Decoder) queryArrayIndex(q *queryResult) error {
	// get the length of the array
	n, err := d.DecodeSliceLen()
	if err != nil {
		return err
	}
	if n == -1 {
		return nil
	}

	// look at all of array elements and recursively call
	// query if need be until we fully digest the key in
	// order to see if we have a match
	if q.key == "*" {
		q.iter++
		//fmt.Printf("\tq.iter = %d <-- WAS JUST INCREMENTED\n", q.iter)
		query := q.query
		for i := 0; i < n; i++ {
			q.query = query
			if err := d.query(q); err != nil {
				return err
			}
		}
		q.iter--
		//fmt.Printf("\tq.iter = %d <-- WAS JUST DECREMENTED\n", q.iter)
		return nil
	}

	// specific index search
	ind, err := strconv.Atoi(q.key)
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		// try to find matching key in array
		if i == ind {
			if err := d.query(q); err != nil {
				return err
			}
			if q.iter > 0 {
				// skip to the next type (array, map, etc.) in outer structure
				return d.skipNext(n - i - 1)
			}
			return nil
		}
		// move (the cursor) to the next index element in the array
		if err := d.Skip(); err != nil {
			return err
		}
	}

	return nil
}

// skips n number of values
func (d *Decoder) skipNext(n int) error {
	for i := 0; i < n; i++ {
		if err := d.Skip(); err != nil {
			return err
		}
	}
	return nil
}

/*
	BEG QUERY EXAMPLE
	=================

	// sample data to marshal (msgpack)
	b, err := msgpack.Marshal([]map[string]interface{}{
		{"id": 1, "attrs": map[string]interface{}{"phone": 12345}},
		{"id": 2, "attrs": map[string]interface{}{"phone": 54321}},
	})
	if err != nil {
		panic(err)
	}

	// open a new decoder
	dec := msgpack.NewDecoder(bytes.NewBuffer(b))

	// execute query on msgpacked data (using decoder)
	values, err := dec.Query("*.attrs.phone")
	if err != nil {
		panic(err)
	}
	fmt.Println("phones are", values) // print results

	// reset decoder's cursor
	dec.Reset(bytes.NewBuffer(b))

	// execute another query on msgpacked data (using decoder)
	values, err = dec.Query("1.attrs.phone")
	if err != nil {
		panic(err)
	}
	fmt.Println("2nd phone is", values[0]) // print single result

	=================
	END QUERY EXAMPLE
*/
