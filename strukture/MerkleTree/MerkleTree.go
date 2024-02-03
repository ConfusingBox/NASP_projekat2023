package strukture

import (
	"bytes"
	"crypto/sha1"
	"os"
	"sort"
)

type MerkleTree struct {
	root     *Node
	elements [][]byte
	leaves   []*Node
}

type Node struct {
	data  [20]byte
	left  *Node
	right *Node
}

func NewNode(data [20]byte) *Node {
	return &Node{data: data, left: nil, right: nil}
}

func NewMerkleTree() *MerkleTree {
	return &MerkleTree{}
}

func (mr *MerkleTree) AddElement(el []byte) {
	mr.elements = append(mr.elements, el)
}

func (mr *MerkleTree) CreateTree(filePath string) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	// Assume each element in the file is a separate entry
	mr.elements = bytes.Split(content, []byte("|"))

	sort.Slice(mr.elements, func(i, j int) bool {
		return bytes.Compare(mr.elements[i], mr.elements[j]) < 0
	})

	mr.buildLeaves()
	mr.buildInternalNodes()
}

func (mr *MerkleTree) CreateTreeWithElems() {
	mr.buildLeaves()
	mr.buildInternalNodes()
}

func (mr *MerkleTree) buildLeaves() {
	for _, el := range mr.elements {
		key := sha1.Sum(el)
		newNode := NewNode(key)
		mr.leaves = append(mr.leaves, newNode)
	}
	mr.padLeaves()
}

func (mr *MerkleTree) padLeaves() {
	for len(mr.leaves)%2 != 0 {
		key := sha1.Sum([]byte{})
		newNode := NewNode(key)
		mr.leaves = append(mr.leaves, newNode)
	}
}

func (mr *MerkleTree) buildInternalNodes() {
	queue := mr.leaves[:]
	for len(queue) > 1 {
		leftN := queue[0]
		rightN := queue[1]
		newData := append(leftN.data[:], rightN.data[:]...)
		queue = queue[2:]
		newNode := NewNode(sha1.Sum(newData))
		newNode.left = leftN
		newNode.right = rightN
		queue = append(queue, newNode)
	}
	mr.root = queue[0]
}

func (mr *MerkleTree) SerializeTree() []byte {
	var result []byte
	queue := []*Node{mr.root}
	for len(queue) > 0 {
		el := queue[0]
		queue = queue[1:]

		// Manually append each byte from el.data1 to result
		for i := 0; i < len(el.data); i++ {
			result = append(result, el.data[i])
		}

		// Append separator '|' if there are more nodes
		if el.left != nil {
			queue = append(queue, el.left)
			result = append(result, '|')
		}
		if el.right != nil {
			queue = append(queue, el.right)
			result = append(result, '|')
		}
	}

	// Remove the trailing '|'
	if len(result) > 0 {
		result = result[:len(result)-1]
	}
	return result
}

func ReconstructTree(data []byte) *MerkleTree {
	keys := bytes.Split(data, []byte("|"))
	keys = keys[:len(keys)-1]

	newMerkleTree := NewMerkleTree()

	nodes := make([]Node, len(keys))

	for i := 0; i < len(keys); i++ {
		copy(nodes[i].data[:], keys[i])
	}

	if len(nodes) > 0 {
		newMerkleTree.root = &nodes[0]
		queue := []*Node{newMerkleTree.root}
		i := 1

		for len(queue) > 0 {
			el := queue[0]
			queue = queue[1:]
			if i < len(nodes) {
				el.left = &nodes[i]
				i++
				if i < len(nodes) {
					el.right = &nodes[i]
					i++
					queue = append(queue, el.left, el.right)
				}
			}
		}
	}

	return newMerkleTree
}
