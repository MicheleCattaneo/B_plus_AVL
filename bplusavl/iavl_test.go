package bplusavl

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestISmallTree(t *testing.T) {
	assert := assert.New(t)
	tree := NewIAVL(int32(4), int32(1))

	keys := [][]byte{{10}, {50}, {30}, {40}, {60}, {20}, {70}, {100}, {80}, {90}}
	values := [][]byte{{10}, {50}, {30}, {40}, {60}, {20}, {70}, {100}, {80}, {90}}

	for i := 0; i < len(keys); i++ {
		tree.Set(keys[i], values[i])
	}

	for i := 0; i < len(keys); i++ {
		assert.NotNil(tree.Get(keys[i]))
		assert.True(bytes.Equal(tree.Get(keys[i]), values[i]))
	}
	oldRootHash := make([]byte, 32)
	copy(oldRootHash, tree.root.hash)

	//tree.root.leftNode.leftNode.keyHeight = 100
	tree.root.completeReHash()
	assert.True(bytes.Equal(oldRootHash, tree.root.hash))

	assert.True(tree.root.isBalancedRecursive())
}

func TestRandomTreeTest(t *testing.T) {
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
	for i := 0; i < size; i++ {
		num := make([]byte, 4)
		binary.LittleEndian.PutUint32(num, uint32(x[i]))
		//fmt.Println(num)
		assert.NotNil(tree.Get(num))
		assert.True(bytes.Equal(tree.Get(num), num))
	}

	// save the old hash value after the insertions, and completely rehash the tree.
	oldRootHash := make([]byte, 32)
	copy(oldRootHash, tree.root.hash)
	tree.root.completeReHash()
	assert.True(bytes.Equal(oldRootHash, tree.root.hash))

	currLeaf := tree.firstLeaf
	elemSum := int32(0)
	for currLeaf != nil {
		elemSum += currLeaf.chunk.GetCurrSize()
		currLeaf = currLeaf.nextLeaf
	}
	assert.Equal(int(elemSum), size)

	assert.Equal(size, int(tree.root.size))
	assert.True(tree.root.isBalancedRecursive())
}
