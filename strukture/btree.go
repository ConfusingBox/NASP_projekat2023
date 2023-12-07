package strukture

import (
	"fmt"
)

type Node struct {
	leaf     bool
	keys     []int
	childPtr []*Node
}

func NewNode(t int, leaf bool) *Node {
	return &Node{
		leaf:     leaf,
		keys:     make([]int, 0),
		childPtr: make([]*Node, 0, t*2),
	}
}

type BTree struct {
	root *Node
	t    int
}

func NewBTree(t int) *BTree {
	return &BTree{
		root: NewNode(t, false),
		t:    t,
	}
}

func (t *BTree) Insert(k int) {
	root := t.root
	if len(root.keys) == (2*t.t - 1) {
		temp := NewNode(t.t, false)
		t.root = temp
		temp.childPtr = append(temp.childPtr, root)
		t.splitChild(temp, 0)
		t.insertNonFull(temp, k)
	} else {
		t.insertNonFull(root, k)
	}
}

func (t *BTree) insertNonFull(x *Node, k int) {
	i := len(x.keys) - 1
	if x.leaf {
		x.keys = append(x.keys, 0)
		for i >= 0 && k < x.keys[i] {
			x.keys[i+1] = x.keys[i]
			i--
		}
		x.keys[i+1] = k
	} else {
		for i >= 0 && k < x.keys[i] {
			i--
		}
		i++
		if i < len(x.childPtr) && len(x.childPtr[i].keys) == (2*t.t-1) {
			t.splitChild(x, i)
			if k > x.keys[i] {
				i++
			}
		}
		if i < len(x.childPtr) {
			t.insertNonFull(x.childPtr[i], k)
		} else {
			// novi cvor ako x.childPtr[i] ne postoji
			newNode := NewNode(t.t, true)
			newNode.keys = append(newNode.keys, k)
			x.childPtr = append(x.childPtr, newNode)
		}
	}
}

func (t *BTree) splitChild(x *Node, i int) {
	tt := t.t
	y := x.childPtr[i]
	z := NewNode(tt, y.leaf)
	x.childPtr = append(x.childPtr, nil)
	copy(x.childPtr[i+2:], x.childPtr[i+1:])
	x.childPtr[i+1] = z
	x.keys = append(x.keys, 0)
	copy(x.keys[i+1:], x.keys[i:])
	x.keys[i] = y.keys[tt-1]
	z.keys = append(z.keys, y.keys[tt:]...)
	y.keys = y.keys[:tt-1]
	if !y.leaf {
		z.childPtr = append(z.childPtr, y.childPtr[tt:]...)
		y.childPtr = y.childPtr[:tt]
	}
}

func (t *BTree) printTree(x *Node, l int) {
	fmt.Printf("Level \"%v\", keys: %v\n", l, x.keys)
	if len(x.childPtr) > 0 {
		l++
		for _, v := range x.childPtr {
			t.printTree(v, l)
		}
	}
}

func (t *BTree) Search(key int) (int, bool) {
	return t.search(t.root, key)
}

func (t *BTree) search(x *Node, k int) (int, bool) {
	i := 0
	for i < len(x.keys) && k > x.keys[i] {
		i++
	}
	if i < len(x.keys) && k == x.keys[i] {
		return x.keys[i], true
	} else if x.leaf {
		return -1, false
	} else {
		if i < len(x.childPtr) {
			return t.search(x.childPtr[i], k)
		} else {
			return -1, false
		}
	}
}

func (t *BTree) getKeys() []int {
	keys := []int{}
	t.getKeysRecursive(t.root, &keys)
	return keys
}

func (t *BTree) getKeysRecursive(x *Node, keys *[]int) {
	if x.leaf {
		*keys = append(*keys, x.keys...)
	} else {
		for i, v := range x.keys {
			if i < len(x.childPtr) {
				t.getKeysRecursive(x.childPtr[i], keys)
			}
			*keys = append(*keys, v)
		}
		if len(x.childPtr) > len(x.keys) {
			t.getKeysRecursive(x.childPtr[len(x.keys)], keys)
		}
	}
}

// brisanje
func (t *BTree) Delete(k int) {
	t.delete(t.root, k)
}

func (t *BTree) delete(x *Node, k int) {
	tt := t.t
	i := 0
	for i < len(x.keys) && k > x.keys[i] {
		i++
	}
	if x.leaf {
		if i < len(x.keys) && x.keys[i] == k {
			x.keys = append(x.keys[:i], x.keys[i+1:]...)
			return
		} else {
			return
		}
	}
	if i < len(x.keys) && x.keys[i] == k {
		if len(x.childPtr[i].keys) >= tt {
			x.keys[i] = t.deletePredecessor(x.childPtr[i])
			return
		} else if len(x.childPtr[i+1].keys) >= tt {
			x.keys[i] = t.deleteSuccessor(x.childPtr[i+1])
			return
		} else {
			t.merge(x, i)
			t.delete(x.childPtr[i], k)
		}
	} else {
		if len(x.childPtr[i].keys) < tt {
			if i > 0 && len(x.childPtr[i-1].keys) >= tt {
				t.borrowFromPrev(x, i)
			} else if i < len(x.keys) && len(x.childPtr[i+1].keys) >= tt {
				t.borrowFromNext(x, i)
			} else {
				if i < len(x.keys) {
					t.merge(x, i)
				} else {
					t.merge(x, i-1)
				}
			}
		}
		t.delete(x.childPtr[i], k)
	}
}

func (t *BTree) deletePredecessor(x *Node) int {
	if x.leaf {
		res := x.keys[len(x.keys)-1]
		x.keys = x.keys[:len(x.keys)-1]
		return res
	} else {
		tt := t.t
		if len(x.childPtr[len(x.childPtr)-1].keys) < tt {
			t.merge(x, len(x.keys)-1)
			return t.deletePredecessor(x.childPtr[len(x.childPtr)-1])
		} else {
			t.borrowFromNext(x, len(x.keys)-1)
			return t.deletePredecessor(x.childPtr[len(x.childPtr)-1])
		}
	}
}

func (t *BTree) deleteSuccessor(x *Node) int {
	if x.leaf {
		res := x.keys[0]
		x.keys = x.keys[1:]
		return res
	} else {
		tt := t.t
		if len(x.childPtr[0].keys) < tt {
			t.merge(x, 0)
			return t.deleteSuccessor(x.childPtr[0])
		} else {
			t.borrowFromPrev(x, 1)
			return t.deleteSuccessor(x.childPtr[0])
		}
	}
}

func (t *BTree) merge(x *Node, i int) {
	y := x.childPtr[i]
	z := x.childPtr[i+1]
	y.keys = append(y.keys, x.keys[i])
	for j := i; j < len(x.keys)-1; j++ {
		x.keys[j] = x.keys[j+1]
		x.childPtr[j+1] = x.childPtr[j+2]
	}
	x.keys = x.keys[:len(x.keys)-1]
	x.childPtr = x.childPtr[:len(x.childPtr)-1]
	y.keys = append(y.keys, z.keys...)
	y.childPtr = append(y.childPtr, z.childPtr...)
}

func (t *BTree) borrowFromPrev(x *Node, i int) {
	y := x.childPtr[i]
	z := x.childPtr[i-1]
	y.keys = append(y.keys, 0)
	copy(y.keys[1:], y.keys)
	y.keys[0] = x.keys[i-1]
	if len(y.childPtr) > 0 {
		y.childPtr = append(y.childPtr, nil)
		copy(y.childPtr[1:], y.childPtr)
		y.childPtr[0] = z.childPtr[len(z.childPtr)-1]
		if len(z.childPtr) > 0 {
			z.childPtr = z.childPtr[:len(z.childPtr)-1]
		}
	}
	x.keys[i-1] = z.keys[len(z.keys)-1]
	z.keys = z.keys[:len(z.keys)-1]
}

func (t *BTree) borrowFromNext(x *Node, i int) {
	y := x.childPtr[i]
	z := x.childPtr[i+1]
	y.keys = append(y.keys, x.keys[i])
	x.keys[i] = z.keys[0]
	z.keys = z.keys[1:]
	if len(z.childPtr) > 0 {
		y.childPtr = append(y.childPtr, z.childPtr[0])
		z.childPtr = z.childPtr[1:]
	}
}

func main() {

	b := NewBTree(3)

	for i := 0; i < 10; i++ {
		b.Insert(i)
	}

	b.printTree(b.root, 0)
	fmt.Println(b.getKeys())

	for i := 0; i < 5; i++ {
		b.Delete(i)
	}

	fmt.Println("")
	b.printTree(b.root, 0)
	fmt.Println(b.getKeys())
}
