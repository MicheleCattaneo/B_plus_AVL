package chunk

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHeapChunk_Serialize(t *testing.T) {
	assert := assert.New(t)

	// make a half full chunk and serialize it
	fullSize := 256
	halfSize := (fullSize / 2) + 1
	chunk := NewHeapChunk(int32(16000000), int32(1024), int32(4), int32(fullSize))
	rand.Seed(time.Now().UnixNano())
	x := rand.Perm(int(halfSize))
	for _, elem := range x {
		num := make([]byte, 4)
		binary.LittleEndian.PutUint32(num, uint32(elem))
		chunk.Insert(num, num)
	}

	var buffer bytes.Buffer
	err := chunk.Serialize(&buffer)
	assert.Nil(err)
	deserializedChunk, err := Deserialize(buffer.Bytes(), int32(fullSize))

	assert.Nil(err)
	assert.NotNil(deserializedChunk)

	assert.Equal(chunk.root, deserializedChunk.root)
	// check root hash
	assert.True(bytes.Equal(chunk.hashes[chunk.root], deserializedChunk.hashes[deserializedChunk.root]))
	assert.Equal(chunk.currKeysNumber, deserializedChunk.currKeysNumber)

	assert.Equal(cap(chunk.keys), cap(deserializedChunk.keys))
	assert.Equal(len(chunk.keys), len(deserializedChunk.keys))

	assert.Equal(len(chunk.values), len(deserializedChunk.values))
	// note: cap for values does not need to match. Not fixed.

	assert.Equal(chunk.nextFreeByte, deserializedChunk.nextFreeByte)
}
