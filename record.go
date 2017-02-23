package godb

import "bytes"

const eofVal byte = 0xC1 // not currently use in the msgpack spec, so we use it for our record data EOF

var (
	maxKey = 24
	maxVal = page - maxKey - 1 // (-1 is for EOF) 4071
)

// data record
type record struct {
	data []byte
	// ==============================================================
	// contains:
	// ==============================================================
	// a fixed length key, reserving a    24 byte section for the key
	// a fixed length val, reserving a 4071+ byte section for the val
	// a fixed length eof, reserving a     1 byte section for the eof
	// =============================================================
	// totaling: 4096+ bytes
	// ==============================================================
}

// create a pointer to a new record
func newRecord(key, val []byte) (*record, error) {
	return &record{append(key, append(val, eofVal)...)}
}

// return key from data record
func (r *record) key() []byte {
	return r.data[:maxKey]
}

// return val from data record
func (r *record) val() []byte {
	if n := bytes.IndexByte(r.data[maxKey:], eofVal); n > -1 {
		return r.data[maxKey : maxKey+n]
	}
	// should we return nil??
	return r.data[maxKey:]
}
