package godb

// database btree node interface
type dbBTreeNode interface {
	hasKey(k []byte) int // returns index of matching key if it exists, otherwise -1
}

// node represents a btree's node of order M.
// if M is 128 a node will occupy 4096 bytes.
// to ensure that a node has only 4096 bytes,
// a fixed sized key of 24 bytes must be used
type node struct {
	numk int
	keys [M - 1][]byte
	ptrs [M]unsafe.Pointer
	rent *node
	leaf bool
}

// create and return a new index node
func newNode() *node {
    return &node{}
}

// create and return a new leaf node
func newLeaf() *node {
    return &node{leaf:true}
}

// checks if a node contains a matching key and
// returns the index of the key, otherwise if it
// does not exist it will return a value of -1.
func (n *node) hasKey(k []byte) int {
	for i := 0; i < n.numk; i++ {
		if bytes.Equal(k, n.keys[i]) {
			return i
		}
	}
	return -1
}