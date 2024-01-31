package bplusavl

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRebuildSmallTree(t *testing.T) {
	assert := assert.New(t)
	tree := NewIAVL(int32(4), int32(1))

	keys := [][]byte{{10}, {50}, {30}, {40}, {60}, {20}, {70}, {100}, {80}, {90}}
	values := [][]byte{{10}, {50}, {30}, {40}, {60}, {20}, {70}, {100}, {80}, {90}}

	for i := 0; i < len(keys); i++ {
		tree.Set(keys[i], values[i])
	}

	var leafList []*Node
	currLeaf := tree.firstLeaf
	for currLeaf != nil {
		leafList = append(leafList, currLeaf)
		currLeaf = currLeaf.nextLeaf
	}

	rebuiltTree := RebuildTree(leafList)
	rebuiltTree.root.completeReHash()
	assert.NotNil(rebuiltTree)
	assert.True(bytes.Equal(tree.root.hash, rebuiltTree.root.hash))
}

func TestRebuildRandomTree(t *testing.T) {
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

	var leafList []*Node
	currLeaf := tree.firstLeaf
	for currLeaf != nil {
		leafList = append(leafList, currLeaf)
		currLeaf = currLeaf.nextLeaf
	}

	rebuiltTree := RebuildTree(leafList)
	rebuiltTree.root.completeReHash()
	assert.NotNil(rebuiltTree)
	assert.True(bytes.Equal(tree.root.hash, rebuiltTree.root.hash))
}
