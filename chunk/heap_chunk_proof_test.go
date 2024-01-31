package chunk

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

	chunk := NewHeapChunk(1024, 256, 1, 8)

	chunk.Insert([]byte{10}, []byte{10})
	chunk.Insert([]byte{30}, []byte{30})
	chunk.Insert([]byte{20}, []byte{20})
	chunk.Insert([]byte{40}, []byte{40})
	chunk.Insert([]byte{70}, []byte{70})
	chunk.Insert([]byte{60}, []byte{60})

	proof, err := chunk.GetProof([]byte{20})
	assert.Nil(err)
	assert.NotNil(proof)
	// fmt.Println(chunk.hashes[chunk.root])
	// fmt.Println(proof.ValidateProof([]byte{20}, []byte{20}))
	assert.True(bytes.Equal(chunk.hashes[chunk.root], proof.ValidateProof([]byte{20}, []byte{20})))

	chunk.Insert([]byte{50}, []byte{50})
	chunk.Insert([]byte{80}, []byte{80})

	proof, err = chunk.GetProof([]byte{60})
	assert.Nil(err)
	assert.NotNil(proof)

	// fmt.Println(chunk.hashes[0])
	// fmt.Println(proof.ValidateProof([]byte{20}, []byte{20}))
	assert.True(bytes.Equal(chunk.hashes[0], proof.ValidateProof([]byte{60}, []byte{60})))
}

func TestBiggerProof(t *testing.T) {
	assert := assert.New(t)

	size := int32(128)
	chunk := NewHeapChunk(16384, 1024, 4, size)

	rand.Seed(time.Now().UnixNano())
	x := rand.Perm(int(size))
	for i, elem := range x {
		num := make([]byte, 4)
		binary.LittleEndian.PutUint32(num, uint32(elem))
		chunk.Insert(num, num)

		// get proof for every element inserted before
		for j := 0; j < i; j++ {
			numToProve := make([]byte, 4)
			binary.LittleEndian.PutUint32(numToProve, uint32(x[j]))

			proof, err := chunk.GetProof(numToProve)
			assert.Nil(err)
			assert.NotNil(proof)

			assert.True(bytes.Equal(proof.ValidateProof(numToProve, numToProve), chunk.hashes[chunk.root]))
		}
	}

	toProve := make([]byte, 4)
	binary.LittleEndian.PutUint32(toProve, uint32(x[40]))

	// get a proof for the selected element
	proof, err := chunk.GetProof(toProve)
	assert.Nil(err)
	assert.NotNil(proof)

	assert.True(bytes.Equal(proof.ValidateProof(toProve, toProve), chunk.hashes[chunk.root]))
	// fmt.Println(chunk.hashes[0])
	// fmt.Println(proof.ValidateProof(toProve, toProve))

	// this element is too big, not in the chunk for sure. Proof should give an error
	_, err = chunk.GetProof([]byte{34, 34, 34, 34})
	assert.NotNil(err)

	// get a proof for an element but evaluate it with a wrong value
	proof, err = chunk.GetProof([]byte{34, 0, 0, 0})
	assert.Nil(err)
	assert.False(bytes.Equal(proof.ValidateProof([]byte{34, 0, 0, 0}, []byte{18, 0, 0, 0}), chunk.hashes[chunk.root]))

}
