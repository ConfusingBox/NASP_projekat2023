package strukture

import (
	"bytes"
	"fmt"
)

// BTreeNode represents a node in the B-Tree.
type BTreeNode struct {
	leaf     bool         // Indicates whether the node is a leaf node.
	keys     [][]byte     // The keys stored in the node.
	values   [][]byte     // The values associated with the keys.
	childPtr []*BTreeNode // Pointers to the child nodes.
}

// NewNode creates a new B-Tree node.
func NewNode(t int, leaf bool) *BTreeNode {
	return &BTreeNode{
		leaf:     leaf,                       // Set whether the node is a leaf node.
		keys:     make([][]byte, 0),          // Initialize the keys slice.
		values:   make([][]byte, 0),          // Initialize the values slice.
		childPtr: make([]*BTreeNode, 0, 2*t), // Initialize the child pointers slice with capacity 2*t.
	}
}

// BTree represents a B-Tree data structure.
type BTree struct {
	root *BTreeNode // The root node of the B-Tree.
	t    int        // The degree of the B-Tree.
}

// NewBTree creates a new B-Tree.
func NewBTree(t int) *BTree {
	return &BTree{
		root: NewNode(t, false), // Create a new root node which is not a leaf node.
		t:    t,                 // Set the degree of the B-Tree.
	}
}

// Insert inserts a key-value pair into the B-Tree.
func (t *BTree) Insert(k, v []byte) {
	// Check if the key already exists in the tree
	_, found := t.Search(k)
	if found {
		fmt.Println("Key already exists in the tree.")
		return
	}

	root := t.root
	// If the root is empty, add the key-value pair to the root
	if len(root.keys) == 0 {
		root.keys = append(root.keys, k)
		root.values = append(root.values, v)
		root.leaf = true
		return
	}
	// If the root is full, split the root and insert the key-value pair
	if len(root.keys) == (2*t.t - 1) {
		temp := NewNode(t.t, false)
		t.root = temp
		temp.childPtr = append(temp.childPtr, root)
		t.splitChild(temp, 0)
		t.insertNonFull(temp, k, v)
	} else {
		t.insertNonFull(root, k, v)
	}
	// If the root has child pointers, it is not a leaf
	if len(root.childPtr) > 0 {
		root.leaf = false
	}
}

// insertNonFull inserts a key-value pair into a non-full node.
func (t *BTree) insertNonFull(x *BTreeNode, k, v []byte) {
	i := len(x.keys) - 1
	// If the node is a leaf, insert the key-value pair into the correct position
	if x.leaf {
		x.keys = append(x.keys, nil)
		x.values = append(x.values, nil)
		for i >= 0 && bytes.Compare(k, x.keys[i]) < 0 {
			x.keys[i+1] = x.keys[i]
			x.values[i+1] = x.values[i]
			i--
		}
		x.keys[i+1] = k
		x.values[i+1] = v
	} else {
		// If the node is not a leaf, find the child pointer to recurse on
		for i >= 0 && bytes.Compare(k, x.keys[i]) < 0 {
			i--
		}
		i++
		// If the child is full, split the child
		if i < len(x.childPtr) && len(x.childPtr[i].keys) == (2*t.t-1) {
			t.splitChild(x, i)
			if bytes.Compare(k, x.keys[i]) > 0 {
				i++
			}
		}
		// Recurse on the appropriate child pointer
		if i < len(x.childPtr) {
			t.insertNonFull(x.childPtr[i], k, v)
		} else {
			// Create a new node if x.childPtr[i] does not exist
			newNode := NewNode(t.t, true)
			newNode.keys = append(newNode.keys, k)
			newNode.values = append(newNode.values, v)
			x.childPtr = append(x.childPtr, newNode)
		}
	}
}

// splitChild splits a full child into two children and updates the parent.
func (t *BTree) splitChild(x *BTreeNode, i int) {
	tt := t.t
	y := x.childPtr[i]
	z := NewNode(tt, y.leaf)
	// Update the parent's child pointers and keys
	x.childPtr = append(x.childPtr, nil)
	copy(x.childPtr[i+2:], x.childPtr[i+1:])
	x.childPtr[i+1] = z
	x.keys = append(x.keys, nil)
	x.values = append(x.values, nil)
	copy(x.keys[i+1:], x.keys[i:])
	copy(x.values[i+1:], x.values[i:])
	x.keys[i] = y.keys[tt-1]
	x.values[i] = y.values[tt-1]
	// Update the new child's keys and child pointers
	z.keys = append(z.keys, y.keys[tt:]...)
	z.values = append(z.values, y.values[tt:]...)
	y.keys = y.keys[:tt-1]
	y.values = y.values[:tt-1]
	if !y.leaf {
		z.childPtr = append(z.childPtr, y.childPtr[tt:]...)
		y.childPtr = y.childPtr[:tt]
	}
}

// PrintTree prints the keys and values of each node in the B-Tree.
func (t *BTree) PrintTree(x *BTreeNode, l int) {
	// Create slices to hold the keys and values as strings
	keys := make([]string, len(x.keys))
	values := make([]string, len(x.values))

	// Convert the keys and values to strings
	for i, v := range x.keys {
		keys[i] = string(v)
	}
	for i, v := range x.values {
		values[i] = string(v)
	}

	// Print the level, keys, and values of the node
	fmt.Printf("Level \"%v\", keys: %v, values: %v\n", l, keys, values)

	// If the node has child pointers, recursively print the children
	if len(x.childPtr) > 0 {
		l++
		for _, v := range x.childPtr {
			t.PrintTree(v, l)
		}
	}
}

// Search searches for a key in the B-Tree.
// It returns the value associated with the key and a boolean indicating whether the key was found.
func (t *BTree) Search(key []byte) ([]byte, bool) {
	// The search starts from the root of the B-Tree.
	return t.searchInNode(t.root, key)
}

// searchInNode searches for a key in a node of the B-Tree.
// It returns the value associated with the key and a boolean indicating whether the key was found.
func (t *BTree) searchInNode(x *BTreeNode, k []byte) ([]byte, bool) {
	i := 0
	// Find the first key greater than or equal to k.
	for i < len(x.keys) && bytes.Compare(k, x.keys[i]) > 0 {
		i++
	}
	// If the key is found in the node, return the value and true.
	if i < len(x.keys) && bytes.Equal(k, x.keys[i]) {
		return x.values[i], true
	} else if x.leaf {
		// If the node is a leaf and the key is not found, return nil and false.
		return nil, false
	} else {
		// If the node is not a leaf, recurse on the appropriate child node.
		if i < len(x.childPtr) {
			return t.searchInNode(x.childPtr[i], k)
		} else {
			return nil, false
		}
	}
}

// Delete deletes a key from the B-Tree.
func (t *BTree) Delete(k []byte) {
	// Check if the key exists in the tree
	_, found := t.Search(k)
	if !found {
		fmt.Println("Ključ ne postoji u stablu.")
		return
	}
	// Delete the key from the tree
	t.deleteNode(t.root, k)

	// If the root is empty, update the root
	if len(t.root.keys) == 0 && len(t.root.childPtr) > 0 {
		t.root = t.root.childPtr[0]
	} else if len(t.root.keys) == 0 {
		t.root = NewNode(t.t, true)
	}
}

// deleteNode deletes a key from a node in the B-Tree.
func (t *BTree) deleteNode(x *BTreeNode, k []byte) {
	tt := t.t
	i := 0

	// Find the first key greater than or equal to k
	for i < len(x.keys) && bytes.Compare(k, x.keys[i]) > 0 {
		i++
	}

	// If the node is a leaf and the key is found, delete the key
	if x.leaf {
		if i < len(x.keys) && bytes.Equal(k, x.keys[i]) {
			x.keys = append(x.keys[:i], x.keys[i+1:]...)
			x.values = append(x.values[:i], x.values[i+1:]...)
			return
		} else {
			return
		}
	}

	// If the node is not a leaf and the key is found, replace the key
	if i < len(x.keys) && bytes.Equal(k, x.keys[i]) {
		if len(x.childPtr[i].keys) >= tt {
			x.keys[i] = t.deletePredecessor(x.childPtr[i])
			x.values[i], _ = t.searchInNode(x.childPtr[i], x.keys[i])
			return
		} else if i+1 < len(x.childPtr) && len(x.childPtr[i+1].keys) >= tt {
			x.keys[i] = t.deleteSuccessor(x.childPtr[i+1])
			x.values[i], _ = t.searchInNode(x.childPtr[i+1], x.keys[i])
			return
		} else {
			t.merge(x, i)
			if i < len(x.childPtr) {
				t.deleteNode(x.childPtr[i], k)
			}
		}
	} else {
		// If the node is not a leaf and the key is not found, recurse on the appropriate child node
		if len(x.childPtr[i].keys) < tt {
			if i > 0 && len(x.childPtr[i-1].keys) >= tt {
				t.borrowFromPrev(x, i)
			} else if i < len(x.keys) && len(x.childPtr[i+1].keys) >= tt {
				t.borrowFromNext(x, i)
			} else {
				if i < len(x.keys) {
					t.merge(x, i)
				} else if i > 0 {
					t.merge(x, i-1)
				}
			}
		}
		if i == len(x.childPtr) {
			i--
		}
		t.deleteNode(x.childPtr[i], k)
	}
}

// deletePredecessor deletes the predecessor of a key in a B-Tree node.
// It returns the key of the predecessor.
func (t *BTree) deletePredecessor(x *BTreeNode) []byte {
	if x.leaf {
		// If the node is a leaf, remove the last key and return it
		res := x.keys[len(x.keys)-1]
		x.keys = x.keys[:len(x.keys)-1]
		x.values = x.values[:len(x.values)-1]
		return res
	} else {
		tt := t.t
		// If the last child has too few keys, merge it with its left sibling
		if len(x.childPtr[len(x.childPtr)-1].keys) < tt {
			t.merge(x, len(x.keys)-1)
			return t.deletePredecessor(x.childPtr[len(x.childPtr)-1])
		} else {
			// Otherwise, borrow a key from the last child's left sibling
			t.borrowFromNext(x, len(x.keys)-1)
			return t.deletePredecessor(x.childPtr[len(x.childPtr)-1])
		}
	}
}

// deleteSuccessor deletes the successor of a key in a B-Tree node.
// It returns the key of the successor.
func (t *BTree) deleteSuccessor(x *BTreeNode) []byte {
	if x.leaf {
		// If the node is a leaf, remove the first key and return it
		res := x.keys[0]
		x.keys = x.keys[1:]
		x.values = x.values[1:]
		return res
	} else {
		tt := t.t
		// If the first child has too few keys, merge it with its right sibling
		if len(x.childPtr[0].keys) < tt {
			t.merge(x, 0)
			return t.deleteSuccessor(x.childPtr[0])
		} else {
			// Otherwise, borrow a key from the first child's right sibling
			t.borrowFromPrev(x, 1)
			return t.deleteSuccessor(x.childPtr[0])
		}
	}
}

// merge merges two children of a B-Tree node.
func (t *BTree) merge(x *BTreeNode, i int) {
	y := x.childPtr[i]
	z := x.childPtr[i+1]
	// Append the key and value from the parent to the left child
	y.keys = append(y.keys, x.keys[i])
	y.values = append(y.values, x.values[i])
	// Shift the keys, values, and child pointers in the parent
	for j := i; j < len(x.keys)-1; j++ {
		x.keys[j] = x.keys[j+1]
		x.values[j] = x.values[j+1]
		if j+2 < len(x.childPtr) {
			x.childPtr[j+1] = x.childPtr[j+2]
		}
	}
	// Remove the last key, value, and child pointer from the parent
	x.keys = x.keys[:len(x.keys)-1]
	x.values = x.values[:len(x.values)-1]
	x.childPtr = x.childPtr[:len(x.childPtr)-1]
	// Append the keys, values, and child pointers from the right child to the left child
	y.keys = append(y.keys, z.keys...)
	y.values = append(y.values, z.values...)
	y.childPtr = append(y.childPtr, z.childPtr...)
}

// borrowFromPrev borrows a key from a B-Tree node's previous sibling.
func (t *BTree) borrowFromPrev(x *BTreeNode, i int) {
	if i == 0 {
		return
	}
	y := x.childPtr[i]
	z := x.childPtr[i-1]
	// Make room for a new key and value in the node
	y.keys = append(y.keys, nil)
	y.values = append(y.values, nil)
	// Shift the keys and values in the node
	copy(y.keys[1:], y.keys)
	copy(y.values[1:], y.values)
	// Borrow the key and value from the parent
	y.keys[0] = x.keys[i-1]
	y.values[0] = x.values[i-1]
	// If the node is not a leaf, also borrow a child pointer
	if len(y.childPtr) > 0 {
		y.childPtr = append(y.childPtr, nil)
		copy(y.childPtr[1:], y.childPtr)
		y.childPtr[0] = z.childPtr[len(z.childPtr)-1]
		if len(z.childPtr) > 0 {
			z.childPtr = z.childPtr[:len(z.childPtr)-1]
		}
	}
	// Replace the parent's key and value with the last key and value from the previous sibling
	x.keys[i-1] = z.keys[len(z.keys)-1]
	x.values[i-1] = z.values[len(z.values)-1]
	// Remove the last key and value from the previous sibling
	z.keys = z.keys[:len(z.keys)-1]
	z.values = z.values[:len(z.values)-1]
}

// borrowFromNext borrows a key from a B-Tree node's next sibling.
func (t *BTree) borrowFromNext(x *BTreeNode, i int) {
	if i == len(x.keys) {
		return
	}
	y := x.childPtr[i]
	z := x.childPtr[i+1]
	// Append the key and value from the parent to the node
	y.keys = append(y.keys, x.keys[i])
	y.values = append(y.values, x.values[i])
	// Replace the parent's key and value with the first key and value from the next sibling
	x.keys[i] = z.keys[0]
	x.values[i] = z.values[0]
	// Remove the first key and value from the next sibling
	z.keys = z.keys[1:]
	z.values = z.values[1:]
	// If the node is not a leaf, also borrow a child pointer
	if len(z.childPtr) > 0 {
		y.childPtr = append(y.childPtr, z.childPtr[0])
		z.childPtr = z.childPtr[1:]
	}
}

// InOrder performs an in-order traversal of the B-Tree.
// It returns a slice of key-value pairs in the order they were visited.
func (t *BTree) InOrder(x *BTreeNode) [][2][]byte {
	var result [][2][]byte
	// The result slice to hold the key-value pairs
	if x != nil {
		for i := 0; i < len(x.keys); i++ {
			if !x.leaf {
				// If the node is not a leaf, recurse on the child pointer
				result = append(result, t.InOrder(x.childPtr[i])...)
			}
			// Append the key-value pair to the result
			result = append(result, [2][]byte{x.keys[i], x.values[i]})
		}
		if !x.leaf {
			// If the node is not a leaf, recurse on the last child pointer
			result = append(result, t.InOrder(x.childPtr[len(x.keys)])...)
		}
	}
	return result
}

// func main() {
// 	// Create a new B-Tree
// 	t := NewBTree(3)

// 	// Keys and values for testing
// 	keys := [][]byte{
// 		[]byte("a"),
// 		[]byte("b"),
// 		[]byte("c"),
// 		[]byte("d"),
// 		[]byte("e"),
// 		[]byte("f"),
// 		[]byte("g"),
// 		[]byte("h"),
// 		[]byte("i"),
// 	}
// 	values := [][]byte{
// 		[]byte("1"),
// 		[]byte("2"),
// 		[]byte("3"),
// 		[]byte("4"),
// 		[]byte("5"),
// 		[]byte("6"),
// 		[]byte("7"),
// 		[]byte("8"),
// 		[]byte("9"),
// 	}

// 	// Insert keys and values into the B-Tree
// 	for i, k := range keys {
// 		t.Insert(k, values[i])
// 	}

// 	// Print the B-Tree
// 	t.PrintTree(t.root, 0)

// 	// Search for keys in the B-Tree
// 	for _, k := range keys {
// 		v, found := t.Search(k)
// 		if found {
// 			fmt.Printf("Key %s found, value: %s\n", k, v)
// 		} else {
// 			fmt.Printf("Key %s not found\n", k)
// 		}
// 	}

// 	// Delete keys from the B-Tree
// 	for _, k := range keys {
// 		t.Delete(k)
// 	}

// 	// Print the B-Tree after deletion
// 	t.PrintTree(t.root, 0)
// }
