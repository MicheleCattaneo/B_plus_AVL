package bplusavl

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSmallProof(t *testing.T) {
	assert := assert.New(t)
	tree := NewIAVL(int32(4), int32(1))

	keys := [][]byte{{10}, {50}, {30}, {40}, {60}, {20}, {70}, {100}, {80}, {90}}
	values := [][]byte{{10}, {50}, {30}, {40}, {60}, {20}, {70}, {100}, {80}, {90}}

	for i := 0; i < len(keys); i++ {
		tree.Set(keys[i], values[i])
	}

	leaf := tree.root.rightNode.leftNode // get some leaf and prepare a proof for it
	proof, _, err := tree.getLeafProof(leaf.chunk.GetSmallestKey())
	assert.Nil(err)
	assert.NotNil(proof)

	// assert that the hash at the root matches the hash returned by validating the proof
	assert.True(bytes.Equal(tree.root.hash, proof.ValidateProof(leaf.hash)))

	// get the proof for a single element
	elemProof, err2 := tree.GetElementProof([]byte{70})
	assert.Nil(err2)
	assert.NotNil(elemProof)
	// validate that that single element is part of the tree
	assert.True(bytes.Equal(tree.root.hash, elemProof.ValidateProof([]byte{70}, []byte{70})))

	_, err3 := tree.GetElementProof([]byte{42})
	assert.NotNil(err3) // assert that there is an error, since the element 42 is not present in the tree

	// test proof when the key is correct but the value is not
	wrongProof, err4 := tree.GetElementProof([]byte{70})
	assert.Nil(err4)
	assert.False(bytes.Equal(tree.root.hash, wrongProof.ValidateProof([]byte{70}, []byte{69})))
}

func TestSingleElementProofInRandomTree(t *testing.T) {
	assert := assert.New(t)
	tree := NewIAVL(int32(8), int32(4))

	size := 15000

	rand.Seed(time.Now().UnixNano())
	x := rand.Perm(size)
	for _, elem := range x {
		num := make([]byte, 4)
		binary.LittleEndian.PutUint32(num, uint32(elem))
		tree.Set(num, num)
	}
	// get the proof for every single element in the tree and validate it
	for _, elem := range x {
		num := make([]byte, 4)
		binary.LittleEndian.PutUint32(num, uint32(elem))
		proof, err := tree.GetElementProof(num)
		assert.Nil(err)
		assert.True(bytes.Equal(tree.root.hash, proof.ValidateProof(num, num)))
		// proof should not work when given a wrong value (like nil)
		assert.False(bytes.Equal(tree.root.hash, proof.ValidateProof(num, nil)))
	}
}

func TestSerializeProof(t *testing.T) {
	assert := assert.New(t)
	tree := NewIAVL(int32(128), int32(4))

	size := 100000

	rand.Seed(time.Now().UnixNano())
	x := rand.Perm(size)
	for _, elem := range x {
		num := make([]byte, 4)
		binary.LittleEndian.PutUint32(num, uint32(elem))
		tree.Set(num, num)
	}

	numberOfChunks := tree.GetNumberOfChunks()

	for i := 0; i < numberOfChunks; i++ {

		proof, chunk, err := tree.GetChunkProof(i)
		assert.Nil(err)

		var buffer bytes.Buffer
		err = proof.SerializeProof(&buffer)
		assert.Nil(err)

		rebuiltProof, err := DeserializeProof(buffer.Bytes())
		assert.Nil(err)

		h := rebuiltProof.ValidateProof(chunk.hash)
		assert.True(bytes.Equal(h, tree.root.hash))
		// fmt.Println(numberOfChunks)
	}
}
