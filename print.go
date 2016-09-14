package godb

import (
	"fmt"
	"strings"
	"unsafe"
)

var queue *print = nil

type print struct {
	node *node
	next *print
}

func newPrint(n *node) *print {
	return &print{n, nil}
}

// utility function to give the length in edges
// for the path from any node to the root
func path_to_root(root, child *node) int {
	var length int
	var c *node = child
	for c != root {
		c = c.rent
		length++
	}
	return length
}

// helper function for printing the
// tree out. (see print_tree)
func enqueue(new_node *node) {
	var c *print
	if queue == nil {
		queue = newPrint(new_node)
		queue.next = nil
	} else {
		c = queue
		for c.next != nil {
			c = c.next
		}
		c.next = newPrint(new_node)
		//new_node.next = nil
	}
}

// helper function for printing the
// tree out. (see print_tree)
func dequeue() *print {
	var n *print = queue
	queue = queue.next
	n.next = nil
	return n
}

// prints the bottom row of keys of the tree
func print_leaves(root *node) {
	fmt.Println("Printing Leaves...")
	var i int
	var c *node = root
	if root == nil {
		fmt.Printf("Empty tree.\n")
		return
	}
	for !c.isLeaf() {
		c = (*node)(unsafe.Pointer(c.ptrs[0]))
	}
	for {
		for i = 0; i < M-1; i++ {
			if c.keys[i] == nil {
				fmt.Printf("___, ")
				continue
			}
			fmt.Printf("%s, ", c.keys[i])
		}
		if c.ptrs[M-1] != nil {
			fmt.Printf(" | ")
			c = (*node)(unsafe.Pointer(c.ptrs[M-1]))
		} else {
			break
		}
	}
	fmt.Printf("\n\n")
}

// print tree out
func print_tree(root *node) {
	fmt.Println("Printing Tree...")
	var i, rank, new_rank int
	if root == nil {
		fmt.Printf("Empty tree.\n")
		return
	}
	queue = nil
	enqueue(root)
	for queue != nil {
		prt := dequeue()
		if prt.node.rent != nil && prt.node == (*node)(unsafe.Pointer(prt.node.rent.ptrs[0])) {
			new_rank = path_to_root(root, prt.node)
			if new_rank != rank {
				rank = new_rank
				fmt.Printf("\n")
			}
		}
		for i = 0; i < prt.node.numk; i++ {
			fmt.Printf("%s, ", prt.node.keys[i])
		}
		if !prt.node.isLeaf() {
			for i = 0; i <= prt.node.numk; i++ {
				enqueue((*node)(unsafe.Pointer(prt.node.ptrs[i])))
			}
		}
		fmt.Printf("| ")
	}
	fmt.Printf("\n\n")
}

func (t *btree) Print() {
	print_tree(t.root)
	//fmt.Println()
	//print_leaves(t.root)
}

func (t *btree) String() string {
	var i, rank, newRank int
	if t.root == nil {
		return "[]"
	}
	queue = nil
	var btree string
	enqueue(t.root)
	btree = "[["
	for queue != nil {
		prt := dequeue()
		if prt.node.rent != nil && prt.node == asNode(prt.node.rent.ptrs[0]) {
			newRank = path_to_root(t.root, prt.node)
			if newRank != rank {
				rank = newRank
				f := strings.LastIndex(btree, ",")
				btree = btree[:f] + btree[f+1:]
				btree += "],["
			}
		}
		btree += "["
		var keys []string
		for i = 0; i < prt.node.numk; i++ {
			keys = append(keys, fmt.Sprintf("%q", prt.node.keys[i]))
		}
		btree += strings.Join(keys, ",")
		if !prt.node.isLeaf() {
			for i = 0; i <= prt.node.numk; i++ {
				enqueue(asNode(prt.node.ptrs[i]))
			}
		}
		btree += "],"
	}
	f := strings.LastIndex(btree, ",")
	btree = btree[:f] + btree[f+1:]
	btree += "]]"
	return btree
}

/* ##### START-ORIGINAL-PRINTER ##### */

// var queue *node = nil
//
// // utility function to give the length in edges
// // for the path from any node to the root
// func path_to_root(root, child *node) int {
// 	var length int
// 	var c *node = child
// 	for c != root {
// 		c = c.rent
// 		length++
// 	}
// 	return length
// }
//
// // helper function for printing the
// // tree out. (see print_tree)
// func enqueue(new_node *node) {
// 	var c *node
// 	if queue == nil {
// 		queue = new_node
// 		queue.next = nil
// 	} else {
// 		c = queue
// 		for c.next != nil {
// 			c = c.next
// 		}
// 		c.next = new_node
// 		new_node.next = nil
// 	}
// }
//
// // helper function for printing the
// // tree out. (see print_tree)
// func dequeue() *node {
// 	var n *node = queue
// 	queue = queue.next
// 	n.next = nil
// 	return n
// }
//
// // prints the bottom row of keys of the tree
// func print_leaves(root *node) {
// 	fmt.Println("Printing Leaves...")
// 	var i int
// 	var c *node = root
// 	if root == nil {
// 		fmt.Printf("Empty tree.\n")
// 		return
// 	}
// 	for !c.isLeaf() {
// 		c = (*node)(unsafe.Pointer(c.ptrs[0]))
// 	}
// 	for {
// 		for i = 0; i < M-1; i++ {
// 			if c.keys[i] == nil {
// 				fmt.Printf("___, ")
// 				continue
// 			}
// 			fmt.Printf("%s, ", c.keys[i])
// 		}
// 		if c.ptrs[M-1] != nil {
// 			fmt.Printf(" | ")
// 			c = (*node)(unsafe.Pointer(c.ptrs[M-1]))
// 		} else {
// 			break
// 		}
// 	}
// 	fmt.Printf("\n\n")
// }
//
// // print tree out
// func print_tree(root *node) {
// 	fmt.Println("Printing Tree...")
// 	var i, rank, new_rank int
// 	if root == nil {
// 		fmt.Printf("Empty tree.\n")
// 		return
// 	}
// 	queue = nil
// 	enqueue(root)
// 	for queue != nil {
// 		n := dequeue()
// 		if n.rent != nil && n == (*node)(unsafe.Pointer(n.rent.ptrs[0])) {
// 			new_rank = path_to_root(root, n)
// 			if new_rank != rank {
// 				rank = new_rank
// 				fmt.Printf("\n")
// 			}
// 		}
// 		for i = 0; i < n.numk; i++ {
// 			fmt.Printf("%s, ", n.keys[i])
// 		}
// 		if !n.isLeaf() {
// 			for i = 0; i <= n.numk; i++ {
// 				enqueue((*node)(unsafe.Pointer(n.ptrs[i])))
// 			}
// 		}
// 		fmt.Printf("| ")
// 	}
// 	fmt.Printf("\n\n")
// }
//
// func (t *btree) Print() {
// 	print_tree(t.root)
// 	//fmt.Println()
// 	//print_leaves(t.root)
// }

// func (t *btree) String() string {
// 	var i, rank, newRank int
// 	if t.root == nil {
// 		return "[]"
// 	}
// 	queue = nil
// 	var btree string
// 	enqueue(t.root)
// 	btree = "[["
// 	for queue != nil {
// 		n := dequeue()
// 		if n.rent != nil && n == asNode(n.rent.ptrs[0]) {
// 			newRank = pathToRoot(t.root, n)
// 			if newRank != rank {
// 				rank = newRank
// 				f := strings.LastIndex(btree, ",")
// 				btree = btree[:f] + btree[f+1:]
// 				btree += "],["
// 			}
// 		}
// 		btree += "["
// 		var keys []string
// 		for i = 0; i < n.numk; i++ {
// 			keys = append(keys, fmt.Sprintf("%q", n.keys[i]))
// 		}
// 		btree += strings.Join(keys, ",")
// 		if !n.isLeaf() {
// 			for i = 0; i <= n.numk; i++ {
// 				enqueue(asNode(n.ptrs[i]))
// 			}
// 		}
// 		btree += "],"
// 	}
// 	f := strings.LastIndex(btree, ",")
// 	btree = btree[:f] + btree[f+1:]
// 	btree += "]]"
// 	return btree
// }

/* ##### END-ORIGINAL-PRINTER ##### */
