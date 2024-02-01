package strukture

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

func (mr *MerkleTree) CreateTree(folderPath string) {
	files, err := ioutil.ReadDir(folderPath)
	if err != nil {
		panic(err)
	}

	var fileContents [][]byte

	for _, file := range files {
		if !file.IsDir() {
			content, err := ioutil.ReadFile(filepath.Join(folderPath, file.Name()))
			if err != nil {
				panic(err)
			}
			fileContents = append(fileContents, content)
		}
	}

	sort.Slice(fileContents, func(i, j int) bool {
		return bytes.Compare(fileContents[i], fileContents[j]) < 0
	})

	mr.elements = fileContents
	mr.buildLeaves()
	mr.buildInternalNodes()
}

// func (mr *MerkleTree) CreateTree() {
// 	mr.buildLeaves()
// 	mr.buildInternalNodes()
// }

func (mr *MerkleTree) buildLeaves() {
	for _, el := range mr.elements {
		key := sha1.Sum(el)
		newNode := NewNode(key)
		mr.leaves = append(mr.leaves, newNode)
	}
	mr.padLeaves()
}

func (mr *MerkleTree) padLeaves() {
	for len(mr.leaves)%4 != 0 {
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

func (mr *MerkleTree) SerializeTree(FILEPATH string) {
	file, err := os.OpenFile(filepath.Join(FILEPATH), os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	queue := []*Node{mr.root}
	for len(queue) > 0 {
		el := queue[0]
		queue = queue[1:]
		file.Write([]byte(hex.EncodeToString(el.data[:])))
		file.Write([]byte("|"))
		if el.left != nil {
			queue = append(queue, el.left)
		}
		if el.right != nil {
			queue = append(queue, el.right)
		}
	}
}

func ReconstructTree(FILEPATH string) *MerkleTree {
	file, err := os.OpenFile(filepath.Join(FILEPATH), os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}
	content, err := ioutil.ReadAll(file)

	keys := strings.Split(string(content), "|")
	keys = keys[:len(keys)-1]

	newMerkleTree := NewMerkleTree()

	nodes := make([]Node, len(keys))

	for i := 0; i < len(keys); i++ {
		u, _ := hex.DecodeString(keys[i])
		var d [20]byte
		copy(d[:], u)
		nodes[i] = Node{data: d}
	}
	i := 1
	newMerkleTree.root = &nodes[0]
	queue := []*Node{newMerkleTree.root}
	for len(queue) > 0 {
		el := queue[0]
		queue = queue[1:]
		if i < len(keys) {
			el.left = &nodes[i]
			i++
			el.right = &nodes[i]
			i++
			queue = append(queue, el.left, el.right)
		}
	}
	return newMerkleTree
}
