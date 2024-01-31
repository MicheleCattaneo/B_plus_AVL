package bplusavl

import (
	hchunk "bplus/chunk"
	"bytes"
	"crypto/sha256"
)

type Node struct {
	key       []byte
	value     []byte
	hash      []byte
	leftHash  []byte
	rightHash []byte

	size      int32
	leftNode  *Node
	rightNode *Node
	height    uint8

	hashIsValid bool
	// inner nodes
	leafPointer *Node
	// leaf nodes
	chunk     *hchunk.HeapChunk
	keyHeight uint8 // assumption: this tree will be kept relatively small (8bit integer)
	leafID    uint32
	nextLeaf  *Node
}

// NewNode returns a new node from a key, value and version.
func NewNode(key []byte, value []byte) *Node {
	return &Node{
		key:    key,
		value:  value,
		height: 0,
		size:   1,
		//version: version,
		hashIsValid: false,
		keyHeight:   0,
	}
}

// public getters

// GetChunkSize returns the size of the underlying chunk contained in this leaf node.
// If node is not a leaf, the function panics.
func (node *Node) GetChunkSize() int32 {
	if node.isLeaf() {
		return node.chunk.GetCurrSize()
	}
	panic("Not a leaf")
}

// GetLeafHash returns a copy of the leaf's hash.
func (node *Node) GetLeafHash() []byte {
	if node.isLeaf() {
		hash := node.hash
		hashCopy := make([]byte, len(hash))

		copy(hashCopy, hash)
		return hashCopy
	}
	return nil
}

func (node *Node) isLeaf() bool {
	return node.height == 0
}

func (node *Node) getLeftNode() *Node {
	return node.leftNode
}

func (node *Node) getRightNode() *Node {
	return node.rightNode
}

// calcHash computes the hash assuming the children have their hash updated
func (node *Node) calcHash() {
	h := sha256.New()
	if node.isLeaf() {
		//h.Write(node.value)
		h.Write([]byte{node.keyHeight}) // add keyHeight to the hash
		h.Write(node.chunk.GetHash())   // add the root hash of the heap to the hash (represents the whole chunk's content)
		node.hash = h.Sum(nil)
		return
	}
	var values []byte
	values = append(values, node.leftNode.hash...)
	values = append(values, node.rightNode.hash...)

	h.Write(values)
	node.hash = h.Sum(nil)
}

// calcHeightAndSize will set the height of the calling node and update the keyHeight of its leaf node.
// It will also update the size of the node.
func (node *Node) calcHeightAndSize() {
	node.height = maxInt8(node.getLeftNode().height, node.getRightNode().height) + 1
	node.leafPointer.keyHeight = node.height
	node.setHashInvalidDownTo(node.leafPointer.chunk.GetSmallestKey())
	node.size = node.getLeftNode().size + node.getRightNode().size
}

func maxInt8(a, b uint8) uint8 {
	if a > b {
		return a
	}
	return b
}

func (node *Node) calcBalance() int {
	return int(node.getLeftNode().height) - int(node.getRightNode().height)
}

func (node *Node) clone() *Node {
	if node.isLeaf() {
		panic("Attempt to copy a leaf node")
	}
	return &Node{
		key:    node.key,
		height: node.height,
		//version:   version,
		size:        node.size,
		hash:        nil,
		leftHash:    node.leftHash,
		leftNode:    node.leftNode,
		rightHash:   node.rightHash,
		rightNode:   node.rightNode,
		hashIsValid: node.hashIsValid,
		leafPointer: node.leafPointer,
		//persisted: false,
	}
}

// recursiveHash recursively computes the hash value of a node if hashIsValid is set to false.
// Otherwise the hash is simply returned.
func (node *Node) recursiveHash() []byte {
	if node.hashIsValid {
		return node.hash
	}
	if node.isLeaf() {
		node.calcHash()
		node.hashIsValid = true
		return node.hash
	}

	h := sha256.New()
	leftH := node.leftNode.recursiveHash()
	rightH := node.rightNode.recursiveHash()

	//var values []byte
	//values = append(values, leftH...)
	//values = append(values, rightH...)
	//
	//h.Write(values)
	h.Write(leftH)
	h.Write(rightH)
	node.hash = h.Sum(nil)
	node.hashIsValid = true
	return node.hash
}

func (node *Node) completeReHash() []byte {

	if node.isLeaf() {
		// FIXME complete rehash the chunks as well?
		node.calcHash()
		node.hashIsValid = true
		return node.hash
	}

	h := sha256.New()
	leftH := node.leftNode.completeReHash()
	rightH := node.rightNode.completeReHash()

	//var values []byte
	//values = append(values, leftH...)
	//values = append(values, rightH...)
	//h.Write(values)
	h.Write(leftH)
	h.Write(rightH)
	node.hash = h.Sum(nil)
	node.hashIsValid = true
	return node.hash
}

// setHashInvalidDownTo sets the hashes to invalid on path from n to targetLeaf.
// This is used when a rotation is performed, and Node.keyHeight is updated in targetLeaf.
// Since this value is part of the hash of targetLeaf, the path needs to be updated.
func (n *Node) setHashInvalidDownTo(targetLeafSmallestKey []byte) {
	if n.isLeaf() {
		//n.calcHash()
		////n.hashIsValid = true // needed??
		n.hashIsValid = false
		return
	}
	if bytes.Compare(targetLeafSmallestKey, n.key) == -1 {
		n.leftNode.setHashInvalidDownTo(targetLeafSmallestKey)
	} else {
		n.rightNode.setHashInvalidDownTo(targetLeafSmallestKey)
	}

	//h := sha256.New()
	//h.Write(n.leftNode.hash)
	//h.Write(n.rightNode.hash)
	//n.hash = h.Sum(nil)
	n.hashIsValid = false // needed?
	return

}

// isBalancedRecursive will check if the tree rooted at the calling node is balanced.
// The tree is defined balanced if the maximal difference between the height of two subtrees
// is at most 1.
func (n *Node) isBalancedRecursive() bool {
	if n.isLeaf() {
		return true
	}
	x, y := n.leftNode.height, n.rightNode.height
	var diff uint8
	if x > y {
		diff = x - y
	} else {
		diff = y - x
	}
	if diff > 1 {
		return false
	}
	return n.leftNode.isBalancedRecursive() && n.rightNode.isBalancedRecursive()
}

func (n *Node) get(key []byte) []byte {
	if n.isLeaf() {
		return n.chunk.Get(key)
	}
	if bytes.Compare(key, n.key) == -1 {
		return n.leftNode.get(key)
	}
	return n.rightNode.get(key)
}
