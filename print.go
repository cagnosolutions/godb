package godb

import (
	"fmt"
	"unsafe"
)

var queue *node = nil

// type print struct {
// 	node
// 	next *node
// }

// func newPrint(n node) {
//
// }

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
	var c *node
	if queue == nil {
		queue = new_node
		queue.next = nil
	} else {
		c = queue
		for c.next != nil {
			c = c.next
		}
		c.next = new_node
		new_node.next = nil
	}
}

// helper function for printing the
// tree out. (see print_tree)
func dequeue() *node {
	var n *node = queue
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
		n := dequeue()
		if n.rent != nil && n == (*node)(unsafe.Pointer(n.rent.ptrs[0])) {
			new_rank = path_to_root(root, n)
			if new_rank != rank {
				rank = new_rank
				fmt.Printf("\n")
			}
		}
		for i = 0; i < n.numk; i++ {
			fmt.Printf("%s, ", n.keys[i])
		}
		if !n.isLeaf() {
			for i = 0; i <= n.numk; i++ {
				enqueue((*node)(unsafe.Pointer(n.ptrs[i])))
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
