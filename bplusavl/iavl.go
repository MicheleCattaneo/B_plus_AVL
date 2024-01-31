package bplusavl

import (
	hchunk "bplus/chunk"
	"bytes"
	"fmt"
	"io"
)

/*
Tendermint iavl
Copyright (C) 2015 Tendermint

This package represents a IAVL tree. The code comes from the tendermint IAVL, and it has been simplified to just execute
rotations (without versioning and storage options).
A IAVL tree is a modified AVL tree that ensures that values are kept at the leaf level so that the balanced tree can be
utilized as a dynamic merkle tree.
Some small additional features have been added later.
				*
               / \
             /     \
           /         \
         /             \
        *               *
       / \             / \
      /   \           /   \
     /     \         /     \
    *       *       *       h6
   / \     / \     / \
  h0  h1  h2  h3  h4  h5
*/

type IAVL struct {
	root       *Node
	maxSize    int32
	chunkSize  int32
	nextLeafID uint32
	firstLeaf  *Node

	chunkList *ChunkList

	keySize           int32
	maxChunkCapacity  int32
	maxChunkValueSize int32
}

func NewIAVL(chunkSize, keySize int32) *IAVL {

	return &IAVL{
		nil,
		0, // TODO put in the arguments
		chunkSize,
		0,
		nil,
		NewEmptyChunkList(),
		keySize,
		int32(16777216), // around 16 MB total chunk size
		int32(65536),    // around 65 kB single value limit
	}
}

// isFull returns true if the tree has reached its maximal capacity.
func (tree *IAVL) isFull() bool {
	return tree.root.size >= tree.maxSize
}

// Get returns the values associated with the given key.
// Careful: the returned values is NOT a copy. Modifying it, causes side effects.
func (tree *IAVL) Get(key []byte) []byte {
	return tree.root.get(key)
}

// GetRootHash returns a copy of the hash value found in the root of the tree.
func (tree *IAVL) GetRootHash() []byte {
	rootHash := tree.root.hash
	rootHashCopy := make([]byte, len(rootHash))

	copy(rootHashCopy, rootHash)
	return rootHashCopy
}

func (tree *IAVL) GetNumberOfChunks() int {
	return tree.chunkList.GetNumberOfChunks()
}

func (tree *IAVL) GetChunk(i int) *Node {
	return tree.chunkList.GetChunk(i)
}

// CorruptData negates the bits of the first byte in the data, so create an invalid chunk.
// Used for testing only.
func (tree *IAVL) CurruptChunkData(i int) {
	tree.chunkList.GetChunk(i).chunk.CorruptData(0) // TODO check if 0 is ok
}

// SerializeChunk serializes the chunk structure (heap_chunk_improved) within a leaf.
// It does not serialize the whole leaf. Use IAVL.SerializeLeafChunk instead.
func (tree *IAVL) SerializeChunk(i int, buffer io.Writer) error {
	return tree.chunkList.GetChunk(i).chunk.Serialize(buffer)
}

// SerializeLeafChunk serializes the whole i-th leaf containing the chunk and metadata.
func (tree *IAVL) SerializeLeafChunk(i int, buffer io.Writer) error {
	return tree.chunkList.GetChunk(i).Serialize(buffer)
}

func (tree *IAVL) CompleteRehash() {
	tree.root.completeReHash()
}

// Set sets a key in the working tree.Nil values are invalid.The given
// key/value byte slices must not be modified after this call, since they point
// to slices stored within IAVL. It returns true when an existing value was
// updated, while false means it was a new key.
func (tree *IAVL) Set(key, value []byte) (updated bool) {
	updated = tree.set(key, value)
	// FIXME uncomment to have hashes
	tree.recursiveHash()
	return updated
}

func (tree *IAVL) set(key []byte, value []byte) bool {
	if value == nil {
		panic(fmt.Sprintf("Attempt to store nil value at key '%s'", key))
	}
	updated := true
	if tree.root == nil {
		leaf := &Node{
			height:      0,
			size:        1,
			hashIsValid: false,
			keyHeight:   0,
			chunk: hchunk.NewHeapChunk(tree.maxChunkCapacity, tree.maxChunkValueSize,
				tree.keySize, tree.chunkSize),
			leafID: tree.nextLeafID,
		}
		tree.nextLeafID += 1
		leaf.chunk.Insert(key, value)

		tree.firstLeaf = leaf
		tree.root = leaf

		tree.chunkList.append(leaf) // add new chunk to the list
		return true
	}

	tree.root, updated = tree.recursiveSet(tree.root, key, value)
	return updated
}

func (tree *IAVL) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool,
) {

	if node.isLeaf() {

		if !node.chunk.IsFull() {
			// if the leaf's chunk has space, simply insert the new KV pair in the chunk
			node.chunk.Insert(key, value)
			node.size += 1
			node.hashIsValid = false
			return node, false
		} else {
			// if the leaf's chunk has no space, it must split into two new chunks for the two new leaves

			leftChunk, middleKey, rightChunk := node.chunk.InsertAndSplit(key, value)
			// left leaf with its new chunk
			node.chunk = leftChunk
			node.size = int32(leftChunk.GetCurrSize())

			// right leaf with its new chunk
			rightLeaf := &Node{
				chunk:       rightChunk,
				keyHeight:   1,
				size:        int32(rightChunk.GetCurrSize()),
				leafID:      tree.nextLeafID,
				hashIsValid: false,
			}
			tree.nextLeafID += 1

			tree.chunkList.append(rightLeaf) // add new chunk to the list

			rightLeaf.nextLeaf = node.nextLeaf
			node.nextLeaf = rightLeaf
			node.hashIsValid = false
			return &Node{
				leftNode:    node,
				rightNode:   rightLeaf,
				key:         middleKey,
				leafPointer: rightLeaf,
				height:      1,
				size:        node.size + rightLeaf.size,
				hashIsValid: false,
			}, false
		}
	} else {
		if bytes.Compare(key, node.key) < 0 {
			node.leftNode, updated = tree.recursiveSet(node.getLeftNode(), key, value)
			node.leftHash = nil // leftHash is yet unknown
			node.hashIsValid = false
		} else {
			node.rightNode, updated = tree.recursiveSet(node.getRightNode(), key, value)
			node.rightHash = nil // rightHash is yet unknown
			node.hashIsValid = false
		}

		if updated {
			return node, updated
		}
		node.calcHeightAndSize()
		newNode := tree.balance(node)
		return newNode, updated
	}
}

func (tree *IAVL) balance(node *Node) (newSelf *Node) {

	balance := node.calcBalance()

	if balance > 1 {
		if node.getLeftNode().calcBalance() >= 0 {
			// Left Left Case
			newNode := tree.rotateRight(node)
			return newNode
		}
		// Left Right Case
		//var leftOrphaned *Node

		left := node.getLeftNode()
		node.leftHash = nil
		node.leftNode = tree.rotateLeft(left)
		newNode := tree.rotateRight(node)
		return newNode
	}
	if balance < -1 {
		if node.getRightNode().calcBalance() <= 0 {
			// Right Right Case
			newNode := tree.rotateLeft(node)
			return newNode
		}
		// Right Left Case
		//var rightOrphaned *Node

		right := node.getRightNode()
		node.rightHash = nil
		node.rightNode = tree.rotateRight(right)
		newNode := tree.rotateLeft(node)

		return newNode
	}
	// Nothing changed
	return node
}

// Rotate right and return the new node and orphan.
func (tree *IAVL) rotateRight(node *Node) *Node {

	// TODO: optimize balance & rotate.
	//node = node.clone()
	orphaned := node.getLeftNode()
	newNode := orphaned

	newNoderHash, newNoderCached := newNode.rightHash, newNode.rightNode
	newNode.rightHash, newNode.rightNode = node.hash, node
	newNode.hashIsValid = false
	node.leftHash, node.leftNode = newNoderHash, newNoderCached
	node.hashIsValid = false

	node.calcHeightAndSize()
	newNode.calcHeightAndSize()

	return newNode
}

// Rotate left and return the new node and orphan.
func (tree *IAVL) rotateLeft(node *Node) *Node {
	// TODO: optimize balance & rotate.
	//node = node.clone()
	orphaned := node.getRightNode()
	newNode := orphaned

	newNodelHash, newNodelCached := newNode.leftHash, newNode.leftNode
	newNode.leftHash, newNode.leftNode = node.hash, node
	newNode.hashIsValid = false
	node.rightHash, node.rightNode = newNodelHash, newNodelCached
	node.hashIsValid = false

	node.calcHeightAndSize()
	newNode.calcHeightAndSize()

	return newNode
}

// recursiveHash recursively computes the hash of the tree from the root.
// Only nodes with 'hashIsValud' set to false have their hashes recomputed.
func (tree *IAVL) recursiveHash() {
	tree.root.recursiveHash()
}

func (tree *IAVL) isBalanced() bool {
	return tree.root.isBalancedRecursive()
}
