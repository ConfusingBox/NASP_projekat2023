package strukture

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

type MerkleTree struct {
	root     *Node
	elements map[string]Entry
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
	return &MerkleTree{
		elements: make(map[string]Entry),
	}
}

func (mr *MerkleTree) AddElement(key string, entry Entry) {
	mr.elements[key] = entry
}

func (mr *MerkleTree) CreateTreeWithElems() {
	keys := make([]string, 0, len(mr.elements))
	for k := range mr.elements {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	mr.buildLeaves(keys)
	mr.buildInternalNodes()
}

func (mr *MerkleTree) CreateTree(filePath string) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	var fileEntries map[string]Entry
	err = json.Unmarshal(content, &fileEntries)
	if err != nil {
		panic(err)
	}

	mr.elements = fileEntries

	keys := make([]string, 0, len(mr.elements))
	for k := range mr.elements {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	mr.buildLeaves(keys)
	mr.buildInternalNodes()
}

func (mr *MerkleTree) buildLeaves(keys []string) {
	for _, key := range keys {
		entry := mr.elements[key]
		data := append([]byte(key), entry.ToByteArray()...)
		keyValueHash := sha1.Sum(data)
		newNode := NewNode(keyValueHash)
		mr.leaves = append(mr.leaves, newNode)
	}
	mr.padLeaves()
}

func (mr *MerkleTree) padLeaves() {
	for len(mr.leaves)%2 != 0 {
		emptyKeyHash := sha1.Sum([]byte{})
		newNode := NewNode(emptyKeyHash)
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

func (mr *MerkleTree) VerifyTree(newElements map[string]Entry) ([]string, error) {
	newTree := NewMerkleTree()
	for key, entry := range newElements {
		newTree.AddElement(key, entry)
	}
	newTree.CreateTreeWithElems()

	if mr.root == nil || newTree.root == nil {
		return nil, fmt.Errorf("one or both trees are empty")
	}

	if !bytes.Equal(mr.root.data[:], newTree.root.data[:]) {
		return compareNodes(mr.root, newTree.root), nil
	}
	return []string{}, nil
}

func compareNodes(oldNode, newNode *Node) []string {
	var changes []string

	if !bytes.Equal(oldNode.data[:], newNode.data[:]) {
		if oldNode.left == nil && oldNode.right == nil {
			changes = append(changes, "Leaf nodes differ")
		} else {
			changes = append(changes, compareNodes(oldNode.left, newNode.left)...)
			changes = append(changes, compareNodes(oldNode.right, newNode.right)...)
		}
	}
	return changes
}

func (mr *MerkleTree) SerializeTree() []byte {
	treeData := struct {
		Elements map[string]Entry `json:"elements"`
		Root     *Node            `json:"root"`
	}{
		Elements: mr.elements,
		Root:     mr.root,
	}

	data, _ := json.Marshal(treeData)

	return data
}

func ReconstructTree(data []byte) *MerkleTree {
	var treeData struct {
		Elements map[string]Entry `json:"elements"`
		Root     *Node            `json:"root"`
	}
	err := json.Unmarshal(data, &treeData)
	if err != nil {
		return nil
	}
	return &MerkleTree{root: treeData.Root, elements: treeData.Elements}
}
