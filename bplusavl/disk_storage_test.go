package bplusavl

import (
	helperfunctions "bplus/helper_functions"
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHeapChunk_Serialize(t *testing.T) {
	assert := assert.New(t)

	dataSize := 256
	size := 10000
	tree := NewIAVL(512, 4)
	rand.Seed(time.Now().UnixNano())
	x := rand.Perm(int(size))
	for i, elem := range x {
		if i == 3 {
			tree.firstLeaf.keyHeight = 42 // randomly put a different keyHeight
		}
		num := make([]byte, 4)
		binary.LittleEndian.PutUint32(num, uint32(elem))

		data := helperfunctions.GetRandomString(dataSize)
		tree.Set(num, data)
	}

	chunkToTake := 4

	// serialize the first (and only) leaf in this tree
	var buffer bytes.Buffer
	// err := tree.firstLeaf.Serialize(&buffer)
	err := tree.SerializeLeafChunk(chunkToTake, &buffer)
	assert.Nil(err)

	// de-serialize it  and check that their hash value match (hence they are the same)
	deserializedLeaf, err2 := Deserialize(buffer.Bytes(), 512)
	assert.Nil(err2)
	assert.True(deserializedLeaf.isLeaf())

	chunkToCompare := tree.GetChunk(chunkToTake)
	assert.True(bytes.Equal(deserializedLeaf.chunk.GetHash(), chunkToCompare.chunk.GetHash()))
	assert.True(bytes.Equal(deserializedLeaf.GetLeafHash(), tree.GetChunk(chunkToTake).GetLeafHash()))
}
