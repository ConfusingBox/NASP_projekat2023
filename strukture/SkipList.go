package strukture

import (
	"bytes"
	"fmt"
	"math/rand"
)

// SkipListNode represents a node in the SkipList.
type SkipListNode struct {
	key   []byte        // The key stored in the node.
	value []byte        // The value associated with the key.
	right *SkipListNode // Pointer to the next node in the same level.
	down  *SkipListNode // Pointer to the node below in the next level.
	level int           // The level of the node.
}

// SkipList represents a SkipList data structure.
type SkipList struct {
	head     *SkipListNode // The head node of the SkipList.
	maxLevel int           // The maximum level of the SkipList.
}

// NewSkipList creates a new SkipList.
func NewSkipList(maxLevel int) *SkipList {
	return &SkipList{
		maxLevel: maxLevel,
	}
}

// roll generates a random level for a new node.
func (s *SkipList) roll() int {
	level := 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxLevel {
			return level
		}
	}
	return level
}

// Insert inserts a key-value pair into the SkipList.
func (s *SkipList) Insert(key []byte, value []byte) {
	level := s.roll()
	newNode := &SkipListNode{key: key, value: value, level: level}
	if s.head == nil {
		s.head = newNode
	} else {
		node := s.head
		for node.right != nil && bytes.Compare(node.right.key, key) < 0 {
			node = node.right
		}
		newNode.right = node.right
		node.right = newNode
	}
}

// Search searches for a key in the SkipList.
// It returns the node containing the key if it exists, otherwise it returns nil.
func (s *SkipList) Search(key []byte) *SkipListNode {
	for node := s.head; node != nil; node = node.down {
		for node.right != nil && bytes.Compare(node.right.key, key) <= 0 {
			node = node.right
		}
		if bytes.Equal(node.key, key) {
			return node
		}
	}
	return nil
}

// Delete deletes a key from the SkipList.
func (s *SkipList) Delete(key []byte) {
	found := false
	for h := s.head; h != nil; h = h.down {
		if bytes.Equal(h.key, key) {
			s.head = h.right
			found = true
		} else {
			for node := h; node.right != nil; node = node.right {
				if bytes.Equal(node.right.key, key) {
					node.right = node.right.right
					found = true
					break
				}
			}
		}
	}
	if !found {
		fmt.Printf("Node with key %s does not exist.\n", string(key))
	}
}

// Print prints the keys and values of the SkipList.
func (s *SkipList) Print() {
	for node := s.head; node != nil; node = node.down {
		for n := node; n != nil; n = n.right {
			fmt.Printf("Key: %s, Value: %s\n", string(n.key), string(n.value))
		}
		fmt.Println()
	}
}

// func main() {
// 	// Create a new SkipList
// 	s := NewSkipList(3)

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

// 	// Insert keys and values into the SkipList
// 	for i, k := range keys {
// 		s.Insert(k, values[i])
// 	}

// 	// Print the SkipList
// 	fmt.Println("SkipList after insertion:")
// 	s.Print()

// 	// Search for keys in the SkipList
// 	for _, k := range keys {
// 		node := s.Search(k)
// 		if node != nil {
// 			fmt.Printf("Key %s found, value: %s\n", node.key, node.value)
// 		} else {
// 			fmt.Printf("Key %s not found\n", k)
// 		}
// 	}

// 	// Delete keys from the SkipList
// 	for _, k := range keys {
// 		s.Delete(k)
// 	}

// 	// Print the SkipList after deletion
// 	fmt.Println("SkipList after deletion:")
// 	s.Print()
// }