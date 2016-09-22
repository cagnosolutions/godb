package godb

import (
	"bytes"
	"sync"
	"unsafe"
)

const M = 16

var (
	nodePool       = sync.Pool{New: func() interface{} { return &node{} }}
	recdPool       = sync.Pool{New: func() interface{} { return &record{} }}
	zero     key_t = [24]byte{}
)

func EncUint32(b []byte) uint32 {
	_ = b[3] // early bounds check; hint to compiler
	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
}

func DecUint32(v uint32) (b []byte) {
	_ = b[3] // early bounds check to guarantee safety of writes below
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
	return
}

type key_t [24]byte

func Key(b []byte) key_t {
	var k key_t
	if len(b) > 24 {
		copy(k[:13], b[:13])
		copy(k[13:], b[(len(b)-13):])
		return k
	}
	copy(k[:], b[:])
	return k
}

func compare(a, b key_t) int {
	return bytes.Compare(a[:], b[:])
}

// node represents a btree's node
type node struct {
	numk int
	keys [M - 1]key_t
	ptrs [M]unsafe.Pointer
	rent *node
	leaf bool
}

func (n *node) hasKey(k key_t) int {
	for i := 0; i < n.numk; i++ {
		if compare(k, n.keys[i]) == 0 {
			return i
		}
	}
	return -1
}

// leaf node record
type record struct {
	key key_t
	val []byte
}

// btree represents the main b+btree
type btree struct {
	root  *node
	count int
	sync.RWMutex
}

func NewBTree() *btree {
	return &btree{}
}

func (t *btree) incr() {
	t.RLock()
	t.count++
	t.RUnlock()
}

func (t *btree) decr() {
	t.RLock()
	t.count--
	t.RUnlock()
}

// Has returns a boolean indicating weather or not the
// provided key and associated record / value exists.
func (t *btree) Has(key key_t) bool {
	_, ptr := t.find(key)
	return ptr != nil
}

// Add inserts a new record using provided key.
// It only inserts if the key does not already exist.
func (t *btree) Add(key key_t, val []byte) {
	// create record ptr for given value
	ptr := &record{key, val}

	// if the btree is empty
	if t.root == nil {
		t.root = startNewbtree(key, ptr)
		t.incr()
		return
	}
	// btree already exists, lets see what we
	// get when we try to find the correct leaf
	leaf := findLeaf(t.root, key)
	// ensure the leaf does not contain the key

	///*
	if leaf.hasKey(key) > -1 {
		return // key already exists, don't add anything
	}
	//*/

	/*
		// hasKey in place
		// ensure the leaf does not contain the key
		for i := 0; i < leaf.numk; i++ {
			if compare(key, leaf.keys[i]) > -1 {
				return // key exists; don't add anything.
			}
		}
	*/

	// btree already exists, and ready to insert into
	if leaf.numk < M-1 {
		insertIntoLeaf(leaf, ptr.key, ptr)
		t.incr()
		return
	}
	// otherwise, insert, split, and balance... returning updated root
	t.incr()
	t.root = insertIntoLeafAfterSplitting(t.root, leaf, ptr.key, ptr)
}

// Set is mainly used for re-indexing
// as it assumes the data to already
// be contained the btree/index. it will
// overwrite duplicate keys, as it does
// not check to see if the key exists...
func (t *btree) Set(key key_t, val []byte) {
	// if the btree is empty, start a new one
	if t.root == nil {
		t.root = startNewbtree(key, &record{key, val})
		t.incr()
		return
	}

	// btree already exists, lets see what we
	// get when we try to find the correct leaf
	leaf := findLeaf(t.root, key)
	// find correct key index, set value and return

	///* ORIG
	if i := leaf.hasKey(key); i > -1 {
		(*record)(unsafe.Pointer(leaf.ptrs[i])).val = val
		return // overwriting existing record
	}
	//*/

	/*
		// hasKey in place
		// find correct key index, set value and return
		for i := 0; i < leaf.numk; i++ {
			if compare(key, leaf.keys[i]) > -1 {
				(*record)(unsafe.Pointer(leaf.ptrs[i])).val = val
				return // overwrite existing record
			}
		}
	*/

	// otherwise, create record ptr for given value
	ptr := &record{key, val} // add a new record
	// if the leaf has room, then insert key and record
	if leaf.numk < M-1 {
		insertIntoLeaf(leaf, ptr.key, ptr)
		t.incr()
		return
	}
	// otherwise, insert, split, and balance... returning updated root
	t.incr()
	t.root = insertIntoLeafAfterSplitting(t.root, leaf, ptr.key, ptr)
}

/*
 *	inserting internals
 */

// first insertion, start a new btree
func startNewbtree(key key_t, ptr *record) *node {
	root := &node{leaf: true}
	root.keys[0] = key
	root.ptrs[0] = unsafe.Pointer(ptr)
	root.ptrs[M-1] = nil
	root.rent = nil
	root.numk++
	return root
}

// creates a new root for two sub-btrees and inserts the key into the new root
func insertIntoNewRoot(left *node, key key_t, right *node) *node {
	root := &node{}
	root.keys[0] = key
	root.ptrs[0] = unsafe.Pointer(left)
	root.ptrs[1] = unsafe.Pointer(right)
	root.numk++
	root.rent = nil
	left.rent = root
	right.rent = root
	return root
}

// insert a new node (leaf or internal) into btree, return root of btree
func insertIntorent(root, left *node, key key_t, right *node) *node {
	if left.rent == nil {
		return insertIntoNewRoot(left, key, right)
	}
	leftIndex := getLeftIndex(left.rent, left)
	if left.rent.numk < M-1 {
		return insertIntoNode(root, left.rent, leftIndex, key, right)
	}
	return insertIntoNodeAfterSplitting(root, left.rent, leftIndex, key, right)
}

// helper->insert_into_rent
// used to find index of the rent's ptr to the
// node to the left of the key to be inserted
func getLeftIndex(rent, left *node) int {
	var leftIndex int
	for leftIndex <= rent.numk && (*node)(unsafe.Pointer(rent.ptrs[leftIndex])) != left {
		leftIndex++
	}
	return leftIndex
}

/*
 *	Inner node insert internals
 */

// insert a new key, ptr to a node
func insertIntoNode(root, n *node, leftIndex int, key key_t, right *node) *node {
	copy(n.ptrs[leftIndex+2:], n.ptrs[leftIndex+1:])
	copy(n.keys[leftIndex+1:], n.keys[leftIndex:])
	n.ptrs[leftIndex+1] = unsafe.Pointer(right)
	n.keys[leftIndex] = key
	n.numk++
	return root
}

// insert a new key, ptr to a node causing node to split
func insertIntoNodeAfterSplitting(root, oldNode *node, leftIndex int, key key_t, right *node) *node {

	var i, j int
	var tmpKeys [M]key_t
	var tmpPtrs [M + 1]unsafe.Pointer

	for i, j = 0, 0; i < oldNode.numk+1; i, j = i+1, j+1 {
		if j == leftIndex+1 {
			j++
		}
		tmpPtrs[j] = oldNode.ptrs[i]
	}

	for i, j = 0, 0; i < oldNode.numk; i, j = i+1, j+1 {
		if j == leftIndex {
			j++
		}
		tmpKeys[j] = oldNode.keys[i]
	}

	tmpPtrs[leftIndex+1] = unsafe.Pointer(right)
	tmpKeys[leftIndex] = key

	split := cut(M)

	newNode := &node{}
	oldNode.numk = 0

	for i = 0; i < split-1; i++ {
		oldNode.ptrs[i] = tmpPtrs[i]
		oldNode.keys[i] = tmpKeys[i]
		oldNode.numk++
	}

	oldNode.ptrs[i] = tmpPtrs[i]

	prime := tmpKeys[split-1]

	for i, j = i+1, 0; i < M; i, j = i+1, j+1 {
		newNode.ptrs[j] = tmpPtrs[i]
		newNode.keys[j] = tmpKeys[i]
		newNode.numk++
	}

	newNode.ptrs[j] = tmpPtrs[i]

	// free tmps...
	for i = 0; i < M; i++ {
		tmpKeys[i] = zero
		tmpPtrs[i] = nil
	}
	tmpPtrs[M] = nil

	newNode.rent = oldNode.rent

	for i = 0; i <= newNode.numk; i++ {
		n := (*node)(unsafe.Pointer(newNode.ptrs[i]))
		n.rent = newNode
	}
	return insertIntorent(root, oldNode, prime, newNode)
}

/*
 *	Leaf node insert internals
 */

// inserts a new key and *record into a leaf, then returns leaf
func insertIntoLeaf(leaf *node, key key_t, ptr *record) {
	var i, insertionPoint int
	for insertionPoint < leaf.numk && compare(leaf.keys[insertionPoint], key) == -1 {
		insertionPoint++
	}
	for i = leaf.numk; i > insertionPoint; i-- {
		leaf.keys[i] = leaf.keys[i-1]
		leaf.ptrs[i] = leaf.ptrs[i-1]
	}
	leaf.keys[insertionPoint] = key
	leaf.ptrs[insertionPoint] = unsafe.Pointer(ptr)
	leaf.numk++
}

// inserts a new key and *record into a leaf, so as
// to exceed the order, causing the leaf to be split
func insertIntoLeafAfterSplitting(root, leaf *node, key key_t, ptr *record) *node {
	// perform linear search to find index to insert new record
	var insertionIndex int
	for insertionIndex < M-1 && compare(leaf.keys[insertionIndex], key) == -1 {
		insertionIndex++
	}
	var tmpKeys [M]key_t
	var tmpPtrs [M]unsafe.Pointer
	var i, j int
	// copy leaf keys & ptrs to temp
	// reserve space at insertion index for new record
	for i, j = 0, 0; i < leaf.numk; i, j = i+1, j+1 {
		if j == insertionIndex {
			j++
		}
		tmpKeys[j] = leaf.keys[i]
		tmpPtrs[j] = leaf.ptrs[i]
	}
	tmpKeys[insertionIndex] = key
	tmpPtrs[insertionIndex] = unsafe.Pointer(ptr)

	leaf.numk = 0
	// index where to split leaf
	split := cut(M - 1)
	// over write original leaf up to split point
	for i = 0; i < split; i++ {
		leaf.ptrs[i] = tmpPtrs[i]
		leaf.keys[i] = tmpKeys[i]
		leaf.numk++
	}
	// create new leaf
	newLeaf := &node{leaf: true}

	// writing to new leaf from split point to end of giginal leaf pre split
	for i, j = split, 0; i < M; i, j = i+1, j+1 {
		newLeaf.ptrs[j] = tmpPtrs[i]
		newLeaf.keys[j] = tmpKeys[i]
		newLeaf.numk++
	}
	// freeing tmps...
	for i = 0; i < M; i++ {
		tmpPtrs[i] = nil
		tmpKeys[i] = zero
	}
	newLeaf.ptrs[M-1] = leaf.ptrs[M-1]
	leaf.ptrs[M-1] = unsafe.Pointer(newLeaf)

	//
	for i = leaf.numk; i < M-1; i++ {
		leaf.keys[i] = zero
		leaf.ptrs[i] = nil
	}
	for i = newLeaf.numk; i < M-1; i++ {
		newLeaf.keys[i] = zero
		newLeaf.ptrs[i] = nil
	}

	newLeaf.rent = leaf.rent
	newKey := newLeaf.keys[0]
	return insertIntorent(root, leaf, newKey, newLeaf)
}

// Get returns the record for
// a given key if it exists
func (t *btree) Get(key key_t) []byte {
	n := findLeaf(t.root, key)
	if n == nil {
		return nil
	}
	var i int
	for i = 0; i < n.numk; i++ {
		if compare(n.keys[i], key) == 0 {
			break
		}
	}
	if i == n.numk {
		return nil
	}
	return (*record)(unsafe.Pointer(n.ptrs[i])).val
}

// find is IDENTICAL TO GET, ONLY IT RETURNS AN UNSAFE.POINTER TO A RECORD
// INSTEAD OF THAT RECORDS VALUE.
//
// returns: leaf node, ptr->record
func (t *btree) find(key key_t) (*node, unsafe.Pointer) {
	leaf := findLeaf(t.root, key)
	if leaf == nil {
		return nil, nil
	}
	var i int
	for i = 0; i < leaf.numk; i++ {
		if compare(leaf.keys[i], key) == 0 {
			break
		}
	}
	if i == leaf.numk {
		return nil, nil
	}
	return leaf, leaf.ptrs[i]
}

/*
 *	Get node internals
 */

// finds and returns leaf node
func findLeaf(root *node, key key_t) *node {
	var c *node = root
	if c == nil {
		return c
	}
	var i int
	for !c.leaf {
		i = 0
		for i < c.numk {
			if compare(key, c.keys[i]) >= 0 {
				i++
			} else {
				break
			}
		}
		c = (*node)(unsafe.Pointer(c.ptrs[i]))
	}
	return c
}

// binary search utility
func search(n *node, key key_t) int {
	lo, hi := 0, n.numk
	for lo < hi {
		md := (lo + hi) >> 1
		if compare(key, n.keys[md]) >= 0 {
			lo = md + 1
		} else {
			hi = md - 1
		}
	}
	return lo
}

/*
// breadth-first-search algorithm, kind of
func (t *btree) BFS() {
	fmt.Println("BFS -- START")
	if t.root == nil {
		return
	}
	c, h := t.root, 0
	for !c.leaf {
		c = (*node)(unsafe.Pointer(c.ptrs[0]))
		h++
	}
	fmt.Printf("h: %d\n[", h)
	for h >= 0 {
		for i := 0; i < M-1; i++ {
			if i == c.numk && c.ptrs[M-1] != nil {
				c = (*node)(unsafe.Pointer(c))
				fmt.Printf(` -> `)
				i = 0
				fmt.Println("\nBFS(1) CONTINUING...")
				continue
			}
			fmt.Println("\nBFS(2) ITERATION...")
			fmt.Printf(`{%s}`, c.keys[i])
		}
		fmt.Println("BFS(3) OUTSIDE INNER FOR LOOP, DECREMENTING 'h'...")
		fmt.Println()
		h--
	}
	fmt.Println("BFS(5) OUTSIDE OF OUTER FOR LOOP...")
	fmt.Printf("]\n")
	fmt.Println("BFS -- DONE")
}
*/

// finds the first leaf in the btree (lexicographically)
func findFirstLeaf(root *node) *node {
	if root == nil {
		return root
	}
	c := root
	for !c.leaf {
		c = (*node)(unsafe.Pointer(c.ptrs[0]))
	}
	return c
}

// Del deletes a record by key
func (t *btree) Del(key key_t) {
	// t.find returns *node (leaf node), and an unsafe.Pointer addressed
	// to a *record, otherwise it will simply return nil for both values
	leaf, uptr := t.find(key)
	if leaf != nil || uptr != nil {
		// double check key, and val
		if r := (*record)(unsafe.Pointer(uptr)); r != nil && compare(r.key, key) == 0 {
			// remove from btree, rebalance, etc.
			t.decr()
			t.root = deleteEntry(t.root, leaf, key, uptr)
		}
	}
}

/*
 * Delete internals
 */

// helper for delete methods... returns index of
// a nodes nearest sibling to the left if one exists
func getNeighborIndex(n *node) int {
	for i := 0; i <= n.rent.numk; i++ {
		if (*node)(unsafe.Pointer(n.rent.ptrs[i])) == n {
			return i - 1
		}
	}
	panic("Search for nonexistent ptr to node in rent.")
}

func removeEntryFromNode(n *node, key key_t, ptr unsafe.Pointer) *node {
	var i, numPtrs int
	// remove key and shift over keys accordingly
	for compare(n.keys[i], key) != 0 {
		i++
	}
	for i++; i < n.numk; i++ {
		n.keys[i-1] = n.keys[i]
	}
	// remove ptr and shift other ptrs accordingly
	// first determine the number of ptrs
	if n.leaf {
		numPtrs = n.numk
	} else {
		numPtrs = n.numk + 1
	}
	i = 0
	for n.ptrs[i] != ptr {
		i++
	}

	for i++; i < numPtrs; i++ {
		n.ptrs[i-1] = n.ptrs[i]
	}
	// one key has been removed
	n.numk--
	// set other ptrs to nil for tidiness; remember leaf
	// nodes use the last ptr to point to the next leaf
	if n.leaf {
		for i := n.numk; i < M-1; i++ {
			n.ptrs[i] = nil
		}
	} else {
		for i := n.numk + 1; i < M; i++ {
			n.ptrs[i] = nil
		}
	}
	return n
}

// deletes an entry from the btree; removes record, key, and ptr from leaf and rebalances btree
func deleteEntry(root, n *node, key key_t, ptr unsafe.Pointer) *node {
	var primeIndex, capacity int
	var neighbor *node
	var prime key_t

	// remove key, ptr from node
	n = removeEntryFromNode(n, key, ptr)

	if n == root {
		return adjustRoot(root)
	}

	var minKeys int
	// case: delete from inner node
	if n.leaf {
		minKeys = cut(M - 1)
	} else {
		minKeys = cut(M) - 1
	}
	// case: node stays at or above min order
	if n.numk >= minKeys {
		return root
	}

	// case: node is bellow min order; coalescence or redistribute
	neighborIndex := getNeighborIndex(n)
	if neighborIndex == -1 {
		primeIndex = 0
	} else {
		primeIndex = neighborIndex
	}
	prime = n.rent.keys[primeIndex]
	if neighborIndex == -1 {
		neighbor = (*node)(unsafe.Pointer(n.rent.ptrs[1]))
	} else {
		neighbor = (*node)(unsafe.Pointer(n.rent.ptrs[neighborIndex]))
	}
	if n.leaf {
		capacity = M
	} else {
		capacity = M - 1
	}

	// coalescence
	if neighbor.numk+n.numk < capacity {
		return coalesceNodes(root, n, neighbor, neighborIndex, prime)
	}
	return redistributeNodes(root, n, neighbor, neighborIndex, primeIndex, prime)
}

func adjustRoot(root *node) *node {
	// if non-empty root key and ptr
	// have already been deleted, so
	// nothing to be done here
	if root.numk > 0 {
		return root
	}
	var newRoot *node
	// if root is empty and has a child
	// promote first (only) child as the
	// new root node. If it's a leaf then
	// the while btree is empty...
	if !root.leaf {
		newRoot = (*node)(unsafe.Pointer(root.ptrs[0]))
		newRoot.rent = nil
	} else {
		newRoot = nil
	}
	root = nil // free root
	return newRoot
}

// merge (underflow)
func coalesceNodes(root, n, neighbor *node, neighborIndex int, prime key_t) *node {
	var i, j, neighborInsertionIndex, nEnd int
	var tmp *node
	// swap neight with node if nod eis on the
	// extreme left and neighbor is to its right
	if neighborIndex == -1 {
		tmp = n
		n = neighbor
		neighbor = tmp
	}
	// starting index for merged pointers
	neighborInsertionIndex = neighbor.numk
	// case nonleaf node, append k_prime and the following ptr.
	// append all ptrs and keys for the neighbors.
	if !n.leaf {
		// append k_prime (key)
		neighbor.keys[neighborInsertionIndex] = prime
		neighbor.numk++
		nEnd = n.numk
		i = neighborInsertionIndex + 1
		j = 0
		for j < nEnd {
			neighbor.keys[i] = n.keys[j]
			neighbor.ptrs[i] = n.ptrs[j]
			neighbor.numk++
			n.numk--
			i++
			j++
		}
		neighbor.ptrs[i] = n.ptrs[j]
		for i = 0; i < neighbor.numk+1; i++ {
			tmp = (*node)(unsafe.Pointer(neighbor.ptrs[i]))
			tmp.rent = neighbor
		}
	} else {
		// in a leaf; append the keys and ptrs.
		i = neighborInsertionIndex
		j = 0
		for j < n.numk {
			neighbor.keys[i] = n.keys[j]
			neighbor.ptrs[i] = n.ptrs[j]
			neighbor.numk++
			i++
			j++
		}
		neighbor.ptrs[M-1] = n.ptrs[M-1]
	}
	root = deleteEntry(root, n.rent, prime, unsafe.Pointer(n))
	n = nil // free n
	return root
}

// merge / redistribute
func redistributeNodes(root, n, neighbor *node, neighborIndex, primeIndex int, prime key_t) *node {
	var i int
	var tmp *node
	// case: node n has a neighnor to the left
	if neighborIndex != -1 {
		if !n.leaf {
			n.ptrs[n.numk+1] = n.ptrs[n.numk]
		}
		for i = n.numk; i > 0; i-- {
			n.keys[i] = n.keys[i-1]
			n.ptrs[i] = n.ptrs[i-1]
		}
		if !n.leaf {
			n.ptrs[0] = neighbor.ptrs[neighbor.numk]
			tmp = (*node)(unsafe.Pointer(n.ptrs[0]))
			tmp.rent = n
			neighbor.ptrs[neighbor.numk] = nil
			n.keys[0] = prime
			n.rent.keys[primeIndex] = neighbor.keys[neighbor.numk-1]
		} else {
			n.ptrs[0] = neighbor.ptrs[neighbor.numk-1]
			neighbor.ptrs[neighbor.numk-1] = nil
			n.keys[0] = neighbor.keys[neighbor.numk-1]
			n.rent.keys[primeIndex] = n.keys[0]
		}
	} else {
		// case: n is left most child (n has no left neighbor)
		if n.leaf {
			n.keys[n.numk] = neighbor.keys[0]
			n.ptrs[n.numk] = neighbor.ptrs[0]
			n.rent.keys[primeIndex] = neighbor.keys[1]
		} else {
			n.keys[n.numk] = prime
			n.ptrs[n.numk+1] = neighbor.ptrs[0]
			tmp = (*node)(unsafe.Pointer(n.ptrs[n.numk+1]))
			tmp.rent = n
			n.rent.keys[primeIndex] = neighbor.keys[0]
		}
		for i = 0; i < neighbor.numk-1; i++ {
			neighbor.keys[i] = neighbor.keys[i+1]
			neighbor.ptrs[i] = neighbor.ptrs[i+1]
		}
		if !n.leaf {
			neighbor.ptrs[i] = neighbor.ptrs[i+1]
		}
	}
	n.numk++
	neighbor.numk--
	return root
}

func destroybtreeNodes(n *node) {
	if n == nil {
		return
	}
	if n.leaf {
		for i := 0; i < n.numk; i++ {
			n.ptrs[i] = nil
		}
	} else {
		for i := 0; i < n.numk+1; i++ {
			destroybtreeNodes((*node)(unsafe.Pointer(n.ptrs[i])))
		}
	}
	n = nil // free
}

// All returns all of the values in the btree (lexicographically)
func (t *btree) All() [][]byte {
	leaf := findFirstLeaf(t.root)
	if leaf == nil {
		return nil
	}
	var vals [][]byte
	for {
		for i := 0; i < leaf.numk; i++ {
			if leaf.ptrs[i] != nil {
				// get record from leaf
				rec := (*record)(unsafe.Pointer(leaf.ptrs[i]))
				// get doc and append to docs
				vals = append(vals, rec.val)
			}
		}
		// we're at the end, no more leaves to iterate
		if leaf.ptrs[M-1] == nil {
			break
		}
		// increment/follow pointer to next leaf node
		leaf = (*node)(unsafe.Pointer(leaf.ptrs[M-1]))
	}
	return vals
}

// Count returns the number of records in the btree
func (t *btree) Count() int {

	return t.count

	// ORIG: BELOW
	// if t.root == nil {
	// 	return 0 // NOTE: used to return -1
	// }
	// c := t.root
	// for !c.leaf {
	// 	c = (*node)(unsafe.Pointer(c.ptrs[0]))
	// }
	// var size int
	// for {
	// 	size += c.numk
	// 	if c.ptrs[M-1] != nil {
	// 		c = (*node)(unsafe.Pointer(c.ptrs[M-1]))
	// 	} else {
	// 		break
	// 	}
	// }
	// t.count = size
	// return t.count
}

// Close destroys all the nodes of the btree
func (t *btree) Close() {
	destroybtreeNodes(t.root)
}

// cut will return the proper
// split point for a node
func cut(length int) int {
	if length%2 == 0 {
		return length / 2
	}
	return length/2 + 1
}
