package godb

import (
	"bytes"
	"fmt"
	"unsafe"
)

const M = 128

// database btree interface
type dbBTree interface {
	open(path string) error
	load(key []byte, blk *block) error
	has(key []byte) bool
	add(key, val []byte) error
	set(key, val []byte) error
	get(key []byte) ([]byte, error)
	del(key []byte) error
	close() error
}

// btree is a b+tree implementation
type btree struct {
	root  *node
	ngin  *engine
	count int
}

// creates a new btree instance and returns it if
// there are no errors encountered while opening
// the mmap'd file (engine) backing the tree on disk.
func (t *btree) open(path string) error {
	t.root = newLeaf()
	t.root.ptrs[M-1] = nil
	t.root.rent = nil
	isnew, err := t.ngin.open(path)
	if err != nil {
		return fmt.Errorf("btree[open]: error opening tree, counld not open engine -> %s", err)
	}
	// reconstruct index if a mapped file already exists
	if !isnew {
		for payload := range t.ngin.loadAllRecords() {
			if err := t.load(payload); err != nil {
				return fmt.Errorf("btree...loading: error while reconstructing: %q", err)
			}
		}
	}
	return nil
}

// Load reconstructs the tree without care to the value
// because we are already provided with the block offset
// and the key via the getAllRecordKeysAndBlocks() method
// supplied to us via the engine.
func (t *btree) load(p payload) error {

	// copy elements from memory mapped payload
	// to new address space to be used. (so it
	// doesn't screw with the memory mapping.)
	key, blk := make([]byte, maxKey), new(block)
	copy(key, p.key)
	blk.pos = p.pos

	// check if key already exists
	leaf, _ := t.find(key)
	if leaf == nil {
		// returned empty leaf node
		return fmt.Errorf("btree[load]: leaf node is nil\n")
	}

	// if room in leaf insert
	if leaf.numk < M-1 {
		// inserting block into leaf (already added record to engine)
		insertIntoLeaf(leaf, key, blk)
		// incrementing record count by one
		t.count++
		return nil
	}
	// otherwise, insert, split, and balance... returning updated root
	t.count++ // incrementing record count by one
	t.root = insertIntoLeafAfterSplitting(t.root, leaf, key, blk)
	return nil
}

// Has returns a boolean indicating weather or not the
// provided key and associated record / value exists.
func (t *btree) has(key []byte) bool {
	_, blk := t.find(key)
	return blk != nil
}

// Add inserts a new record using provided key.
// It only inserts if the key does not already exist.
func (t *btree) add(key []byte, val []byte) error {
	// check if key already exists
	leaf, blk := t.find(key)
	if leaf == nil {
		// returned empty leaf node
		return fmt.Errorf("btree[add]: leaf node is nil\n")
	}
	if blk != nil {
		// key already exists, don't add!
		return fmt.Errorf("btree[add]: key already exists, not adding\n")
	}
	blk = new(block)
	// key does not exist. add into engine
	pos, err := t.ngin.addRecord(newRecord(key, val))
	if err != nil {
		// failed to add record to engine
		return fmt.Errorf("btree[add]: failed to add record to engine -> %s", err)
	}
	blk.pos = pos
	// if room in leaf insert
	if leaf.numk < M-1 {
		// inserting block into leaf (already added record to engine)
		insertIntoLeaf(leaf, key, blk)
		// incrementing record count by one
		t.count++
		return nil
	}
	// otherwise, insert, split, and balance... returning updated root
	t.count++ // incrementing record count by one
	t.root = insertIntoLeafAfterSplitting(t.root, leaf, key, blk)
	return nil
}

// Set is mainly used for re-indexing
// as it assumes the data to already
// be contained the btree/index. it will
// overwrite duplicate keys, as it does
// not check to see if the key exists...
func (t *btree) set(key []byte, val []byte) error {
	// check if key already exists
	leaf, blk := t.find(key)
	if leaf == nil {
		return fmt.Errorf("btree[set]: leaf node is nil\n")
	}
	// check if key exists in tree
	if blk != nil {
		// key exists in tree, update engine
		t.ngin.setRecord(blk.pos, newRecord(key, val))
		return nil
	}
	// key does not exist. add into engine
	blk = new(block)
	pos, err := t.ngin.addRecord(newRecord(key, val))
	if err != nil {
		// failed to add to engine
		return fmt.Errorf("btree[set]: failed to add to engine -> %s", err)
	}
	blk.pos = pos
	// if room in leaf insert
	if leaf.numk < M-1 {
		// insert block into leaf (already added record to engine)
		insertIntoLeaf(leaf, key, blk)
		// increent record count by one
		t.count++
		return nil
	}
	// otherwise, insert, split, and balance... returning updated root
	t.count++ // incrementing record count by one
	t.root = insertIntoLeafAfterSplitting(t.root, leaf, key, blk)
	return nil
}

/*
 *	inserting internals
 */

// creates a new root for two sub-btrees and inserts the key into the new root
func insertIntoNewRoot(left *node, key []byte, right *node) *node {
	root := newNode()
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
func insertIntoRent(root, left *node, key []byte, right *node) *node {
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
func insertIntoNode(root, n *node, leftIndex int, key []byte, right *node) *node {
	copy(n.ptrs[leftIndex+2:], n.ptrs[leftIndex+1:])
	copy(n.keys[leftIndex+1:], n.keys[leftIndex:])
	n.ptrs[leftIndex+1] = unsafe.Pointer(right)
	n.keys[leftIndex] = key
	n.numk++
	return root
}

// insert a new key, ptr to a node causing node to split
func insertIntoNodeAfterSplitting(root, oldNode *node, leftIndex int, key []byte, right *node) *node {

	var i, j int
	var tmpKeys [M][]byte
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

	newNode := newNode()
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
		tmpKeys[i] = nil
		tmpPtrs[i] = nil
	}
	tmpPtrs[M] = nil

	newNode.rent = oldNode.rent

	for i = 0; i <= newNode.numk; i++ {
		n := (*node)(unsafe.Pointer(newNode.ptrs[i]))
		n.rent = newNode
	}
	return insertIntoRent(root, oldNode, prime, newNode)
}

/*
 *	Leaf node insert internals
 */

// inserts a new key and *block into a leaf
func insertIntoLeaf(leaf *node, key []byte, blk *block) {
	var i, pos int
	for pos < leaf.numk && bytes.Compare(leaf.keys[pos], key) == -1 {
		pos++
	}

	for i = leaf.numk; i > pos; i-- {
		leaf.keys[i] = leaf.keys[i-1]
		leaf.ptrs[i] = leaf.ptrs[i-1]
	}

	// leaf.engine.set(writeRecord(ptr), pos)

	// assumed value is already inserted into engine
	leaf.keys[pos] = key
	leaf.ptrs[pos] = unsafe.Pointer(blk)
	leaf.numk++
}

// inserts a new key and *block into a leaf, so as
// to exceed the order, causing the leaf to be split
func insertIntoLeafAfterSplitting(root, leaf *node, key []byte, blk *block) *node {
	// perform linear search to find index to insert new record
	var pos int
	for pos < M-1 && bytes.Compare(leaf.keys[pos], key) == -1 {
		pos++
	}
	var tmpKeys [M][]byte
	var tmpPtrs [M]unsafe.Pointer
	var i, j int
	// copy leaf keys & ptrs to temp
	// reserve space at insertion index for new record
	for i, j = 0, 0; i < leaf.numk; i, j = i+1, j+1 {
		if j == pos {
			j++
		}
		tmpKeys[j] = leaf.keys[i]
		tmpPtrs[j] = leaf.ptrs[i]
	}
	tmpKeys[pos] = key
	tmpPtrs[pos] = unsafe.Pointer(blk)

	leaf.numk = 0
	// index where to split leaf
	split := cut(M - 1)
	// over write original leaf up to split point
	for i = 0; i < split; i++ {
		leaf.keys[i] = tmpKeys[i]
		leaf.ptrs[i] = tmpPtrs[i]
		leaf.numk++
	}
	// create new leaf
	newLeaf := newLeaf()

	// writing to new leaf from split point to end of giginal leaf pre split
	for i, j = split, 0; i < M; i, j = i+1, j+1 {
		newLeaf.keys[j] = tmpKeys[i]
		newLeaf.ptrs[j] = tmpPtrs[i]
		newLeaf.numk++
	}
	// freeing tmps...
	for i = 0; i < M; i++ {
		tmpKeys[i] = nil
		tmpPtrs[i] = nil
	}
	newLeaf.ptrs[M-1] = leaf.ptrs[M-1]
	leaf.ptrs[M-1] = unsafe.Pointer(newLeaf)

	// wipe old and new leaf
	for i = leaf.numk; i < M-1; i++ {
		leaf.keys[i] = nil
		leaf.ptrs[i] = nil
	}
	for i = newLeaf.numk; i < M-1; i++ {
		newLeaf.keys[i] = nil
		newLeaf.ptrs[i] = nil
	}

	newLeaf.rent = leaf.rent
	newKey := newLeaf.keys[0]
	return insertIntoRent(root, leaf, newKey, newLeaf)
}

// Get returns the record for
// a given key if it exists
func (t *btree) get(key []byte) ([]byte, error) {
	if _, blk := t.find(key); blk != nil {
		val, err := t.ngin.getRecordVal(blk.pos)
		if err != nil {
			return nil, fmt.Errorf("btree[get]: failed to get record from engine -> %s", err)
		}
		return val, nil
	}
	return nil, fmt.Errorf("btree[get]: failed to get block from leaf\n")
}

// returns: leaf node, block
func (t *btree) find(key []byte) (*node, *block) {
	leaf := findLeaf(t.root, key)
	if leaf == nil {
		return nil, nil
	}
	var i int
	for i = 0; i < leaf.numk; i++ {
		if bytes.Equal(leaf.keys[i], key) {
			break
		}
	}
	if i == leaf.numk {
		return leaf, nil
	}
	return leaf, (*block)(unsafe.Pointer(leaf.ptrs[i]))
}

/*
 *	Get node internals
 */

// finds and returns leaf node
func findLeaf(root *node, key []byte) *node {
	var c *node = root
	if c == nil {
		return c
	}

	var i int
	for !c.leaf {
		i = 0
		for i < c.numk {
			if bytes.Compare(key, c.keys[i]) >= 0 {
				i++
			} else {
				break
			}
		}
		c = (*node)(unsafe.Pointer(c.ptrs[i]))
	}
	// this is the found leaf node
	return c
}

// Del deletes a record by key
func (t *btree) del(key []byte) error {
	leaf, blk := t.find(key)
	err := fmt.Errorf("btree[del]: failed to locate proper leaf or block (leaf: %+v, block: %+v)", leaf, blk)
	if blk != nil && leaf != nil {
		// delete record from engine
		if err := t.ngin.delRecord(blk.pos); err != nil {
			return fmt.Errorf("btree[del]: faied to delete record from engine -> %s", err)
		}
		// delete index block from tree
		t.count--
		t.root = deleteEntry(t.root, leaf, key, unsafe.Pointer(blk))
		blk, err = nil, nil
	}
	return err
	/*
		// t.find returns *node (leaf node), and a *block
		// otherwise it will simply return nil for both values
		leaf, blk := t.find(key)

		if leaf == nil {
			return fmt.Errorf("btree[del]: failed to find leaf node")
		}
		if blk == nil {
			return fmt.Errorf("btree[del]: failed to find block in leaf node")
		}
		// double check passed key, and OLD records key
		A
		rkey, err := t.ngin.getRecordKey(blk.pos)
		if err != nil {
			return fmt.Errorf("btree[del]: failed to get record from engine -> %s", err)
		}
		// ensure they match...
		if !bytes.Equal(rkey, key) {
			fmt.Printf("check key matches:\n\t%q\n%q\n", rkey, key)
			return fmt.Errorf("btree[del]: key does not match record key on disk at same block position")
		}
		// so far, so good... attempt to delete record from the mapped engine
		if err := t.ngin.delRecord(blk.pos); err != nil {
			return fmt.Errorf("btree[del]: failed to delete record from engine -> %s", err)
		}
		// success!
		// remove from btree, rebalance, etc.
		t.count-- // decrementing record count by one
		t.root = deleteEntry(t.root, leaf, key, unsafe.Pointer(blk))
		return nil
	*/
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
	panic("btree[getNeighborIndex]: (panic) search for nonexistent ptr to node in rent.")
}

func removeEntryFromNode(n *node, key []byte, ptr unsafe.Pointer) *node {
	var i, numPtrs int
	// remove key and shift over keys accordingly
	for n.keys[i] != nil && !bytes.Equal(n.keys[i], key) {
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
	//for !reflect.DeepEqual((*uintptr)(unsafe.Pointer(n.ptrs[i])), (*uintptr)(unsafe.Pointer(ptr))) {
	//	i++
	//}

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
func deleteEntry(root, n *node, key []byte, ptr unsafe.Pointer) *node {

	var primeIndex, minKeys, capacity int
	var neighbor *node
	var prime []byte

	// remove key and ptr from node
	n = removeEntryFromNode(n, key, ptr)

	// case: deletion from the root
	if n == root {
		return adjustRoot(root)
	}

	// case: delete from node below root (rest of funtion body)
	if n.leaf {
		minKeys = cut(M - 1)
	} else {
		minKeys = cut(M) - 1
	}

	// case: node stays at or above minimum
	if n.numk >= minKeys {
		return root
	}

	// case: node is below minimum order, a coalescence or redistribution is needed...

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

	// redistrubution
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
func coalesceNodes(root, n, neighbor *node, neighborIndex int, prime []byte) *node {
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
func redistributeNodes(root, n, neighbor *node, neighborIndex, primeIndex int, prime []byte) *node {
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

// Close destroys all the nodes of the btree
func (t *btree) close() error {
	destroybtreeNodes(t.root)
	if err := t.ngin.close(); err != nil {
		return fmt.Errorf("btree[close]: error encountered while closing engine -> %s", err)
	}
	t.count = 0
	return nil
}

// cut will return the proper
// split point for a node
func cut(length int) int {
	if length%2 == 0 {
		return length / 2
	}
	return length/2 + 1
}

/* UNKNOWN IF WE NEED YET OR NOT */

// All returns all of the values in the btree (lexicographically)

/*
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
*/

// binary search utility

/*
func search(n *node, key []byte) int {
	lo, hi := 0, n.numk
	for lo < hi {
		md := (lo + hi) >> 1
		if bytes.Compare(key, n.keys[md]) >= 0 {
			lo = md + 1
		} else {
			hi = md - 1
		}
	}
	return lo
}
*/

// finds the first leaf in the btree (lexicographically)

func (t *btree) findFirstLeaf() *node {
	if t.root == nil {
		return t.root
	}
	c := t.root
	for !c.leaf {
		c = (*node)(unsafe.Pointer(c.ptrs[0]))
	}
	return c
}

func (t *btree) next() <-chan []byte {
	n := t.findFirstLeaf()
	if n == nil {
		return nil
	}
	v := make(chan []byte)
	go func() {
		for {
			for i := 0; i < n.numk; i++ {
				if blk := n.getBlock(i); blk != nil {
					val, err := t.ngin.getRecordVal(blk.pos)
					if err != nil {
						v <- nil
					}
					v <- val
				}
			}
			if n = n.next(); n == nil {
				break
			}
		}
		close(v)
	}()
	return v
}

// first insertion, start a new btree

/*
func startNewbtree(key []byte, blk *block) *node {
	root := newLeaf()
	root.keys[0] = key
	root.ptrs[0] = unsafe.Pointer(blk)
	root.ptrs[M-1] = nil
	root.rent = nil
	root.numk++
	return root
}
*/
