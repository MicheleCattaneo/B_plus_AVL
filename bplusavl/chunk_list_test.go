package bplusavl

import (
	hchunk "bplus/chunk"
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSmallEmptyChunkList(t *testing.T) {
	assert := assert.New(t)
	chunks := NewEmptyChunkList()

	assert.NotNil(chunks)

	l1 := &Node{
		height: 0,
		chunk:  hchunk.NewHeapChunk(1204, 128, 4, 16),
		leafID: 1,
	}
	l1.chunk.Insert([]byte{10, 10, 10, 10}, []byte("Element one"))

	l2 := &Node{
		height: 0,
		chunk:  hchunk.NewHeapChunk(1204, 128, 4, 16),
		leafID: 2,
	}
	l2.chunk.Insert([]byte{40, 40, 40, 40}, []byte("Element two"))

	l3 := &Node{
		height: 0,
		chunk:  hchunk.NewHeapChunk(1204, 128, 4, 16),
		leafID: 3,
	}
	l3.chunk.Insert([]byte{20, 20, 20, 20}, []byte("Element three"))

	l4 := &Node{
		height: 0,
		chunk:  hchunk.NewHeapChunk(1204, 128, 4, 16),
		leafID: 4,
	}
	l4.chunk.Insert([]byte{5, 5, 5, 5}, []byte("Element four"))

	l5 := &Node{
		height: 0,
		chunk:  hchunk.NewHeapChunk(1204, 128, 4, 16),
		leafID: 5,
	}
	l5.chunk.Insert([]byte{70, 70, 70, 70}, []byte("Element five"))

	chunks.append(l1)
	assert.Equal(uint32(1), chunks.GetChunk(0).leafID)

	chunks.append(l2)
	assert.Equal(uint32(1), chunks.GetChunk(0).leafID)
	assert.Equal(uint32(2), chunks.GetChunk(1).leafID)

	chunks.append(l3)
	assert.Equal(uint32(1), chunks.GetChunk(0).leafID)
	assert.Equal(uint32(3), chunks.GetChunk(1).leafID)
	assert.Equal(uint32(2), chunks.GetChunk(2).leafID)

	chunks.append(l4)
	assert.Equal(uint32(4), chunks.GetChunk(0).leafID)
	assert.Equal(uint32(1), chunks.GetChunk(1).leafID)
	assert.Equal(uint32(3), chunks.GetChunk(2).leafID)
	assert.Equal(uint32(2), chunks.GetChunk(3).leafID)

	chunks.append(l5)
	assert.Equal(uint32(4), chunks.GetChunk(0).leafID)
	assert.Equal(uint32(1), chunks.GetChunk(1).leafID)
	assert.Equal(uint32(3), chunks.GetChunk(2).leafID)
	assert.Equal(uint32(2), chunks.GetChunk(3).leafID)
	assert.Equal(uint32(5), chunks.GetChunk(4).leafID)

	assert.Nil(chunks.GetChunk(100))

	// TODO: create leaves and add at least one key to each chunk
	// insert them in the list and make sure that they are sorted
}

// Test
func TestRandomTreeChunkListAndProofs(t *testing.T) {
	assert := assert.New(t)
	tree := NewIAVL(int32(16), int32(4))
	size := 10000

	rand.Seed(time.Now().UnixNano())
	x := rand.Perm(size)
	for _, elem := range x {
		num := make([]byte, 4)
		binary.LittleEndian.PutUint32(num, uint32(elem))
		tree.Set(num, num)
	}

	assert.NotNil(tree.chunkList)
	assert.Equal(uint32(len(tree.chunkList.chunks)), tree.nextLeafID)

	// assert that chunks are sorted in the list
	for i := 1; i < len(tree.chunkList.chunks); i++ {
		c1 := tree.chunkList.GetChunk(i - 1)
		c2 := tree.chunkList.GetChunk(i)

		assert.True(bytes.Compare(c1.chunk.GetSmallestKey(), c2.chunk.GetSmallestKey()) == -1)
	}

	// check that total size contained in the list chunk
	// corresponds to total size
	totSize := int32(0)
	for i := 0; i < len(tree.chunkList.chunks); i++ {
		totSize += tree.chunkList.GetChunk(i).chunk.GetCurrSize()
	}
	assert.Equal(totSize, tree.root.size)

	// check proofs for each chunk
	for i := 0; i < len(tree.chunkList.chunks); i++ {
		proof, node, err := tree.GetChunkProof(i)
		assert.Nil(err)
		assert.True(node.leafID == tree.chunkList.GetChunk(i).leafID)
		assert.NotNil(proof)

		rootHash := proof.ValidateProof(node.hash)
		// fmt.Println(rootHash)
		// fmt.Println(tree.root.hash)
		assert.True(bytes.Equal(rootHash, tree.root.hash))
	}
}
