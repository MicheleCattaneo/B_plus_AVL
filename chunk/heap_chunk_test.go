package chunk

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	// "time"
)

func TestMakeSmallHeap(t *testing.T) {
	assert := assert.New(t)

	chunk := NewHeapChunk(int32(256), int32(16), int32(4), int32(16))

	assert.NotNil(chunk)
	assert.Equal(chunk.indexBytes, int32(1))
	assert.Equal(chunk.sizeBytes, int32(1))

	k1 := []byte{20, 0, 5, 2}
	i1 := chunk.getInsertionIndex(k1)

	assert.Equal(int32(0), i1)
	assert.Equal(int32(0), chunk.indexToByte(i1))

	chunk.Insert([]byte{20, 20, 20, 20}, []byte("Ciao mamma"))
	chunk.Insert([]byte{10, 10, 10, 10}, []byte("Ciao"))
	chunk.Insert([]byte{30, 30, 30, 30}, []byte("Bello"))

	fmt.Println(chunk.keys)

	val := chunk.Get([]byte{30, 30, 30, 30})
	assert.True(bytes.Equal([]byte("Bello"), val))

	val = chunk.Get([]byte{20, 20, 20, 20})
	assert.True(bytes.Equal([]byte("Ciao mamma"), val))

	val = chunk.Get([]byte{10, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Ciao"), val))
}

func TestChildRelationship(t *testing.T) {
	assert := assert.New(t)

	chunk := NewHeapChunk(int32(256), int32(16), int32(4), int32(8))
	chunk.root = 6
	assert.Equal(int32(7), leftChildOffset(6, chunk.root))
	assert.Equal(int32(8), rightChildOffset(6, chunk.root))

	chunk.root = 5
	assert.Equal(int32(6), leftChildOffset(5, chunk.root))
	assert.Equal(int32(7), rightChildOffset(5, chunk.root))
	assert.Equal(int32(8), leftChildOffset(6, chunk.root))
	assert.Equal(int32(9), rightChildOffset(6, chunk.root))

	chunk.root = 4
	assert.Equal(int32(5), leftChildOffset(4, chunk.root))
	assert.Equal(int32(6), rightChildOffset(4, chunk.root))
	assert.Equal(int32(7), leftChildOffset(5, chunk.root))
	assert.Equal(int32(8), rightChildOffset(5, chunk.root))
	assert.Equal(int32(9), leftChildOffset(6, chunk.root))
	assert.Equal(int32(10), rightChildOffset(6, chunk.root))

	chunk.root = 3
	assert.Equal(int32(4), leftChildOffset(3, chunk.root))
	assert.Equal(int32(5), rightChildOffset(3, chunk.root))
	assert.Equal(int32(6), leftChildOffset(4, chunk.root))
	assert.Equal(int32(7), rightChildOffset(4, chunk.root))
	assert.Equal(int32(8), leftChildOffset(5, chunk.root))
	assert.Equal(int32(9), rightChildOffset(5, chunk.root))
	assert.Equal(int32(10), leftChildOffset(6, chunk.root))
	assert.Equal(int32(11), rightChildOffset(6, chunk.root))
	assert.Equal(int32(12), leftChildOffset(7, chunk.root))
	assert.Equal(int32(13), rightChildOffset(7, chunk.root))

}

// Test the insertion of some random values with random sizes
func TestRandomValueSize(t *testing.T) {

	assert := assert.New(t)
	min := 256
	max := 1024

	rand.Seed(time.Now().UnixNano())
	chunkSize := 64
	randomStrings := make([][]byte, chunkSize)
	totalValueSize := 0
	for i := 0; i < chunkSize; i++ {
		// Generate random string of random size
		strlen := rand.Intn(max-min) + min
		totalValueSize += strlen
		str := getRandomString(strlen)
		randomStrings[i] = str
	}
	chunk := NewHeapChunk(int32(16000000), int32(max), int32(4), int32(chunkSize))

	// INSERT
	x := rand.Perm(chunkSize)
	for _, elem := range x {
		num := make([]byte, 4)
		binary.LittleEndian.PutUint32(num, uint32(elem))
		chunk.Insert(num, randomStrings[elem])

		// save old hash, recompute it and make sure it stays the same
		oldHash := make([]byte, 32)
		copy(oldHash, chunk.hashes[chunk.root])
		chunk.computeHashes()
		assert.True(bytes.Equal(oldHash, chunk.hashes[chunk.root]))
	}
	// RETRIEVE
	for _, elem := range x {
		num := make([]byte, 4)
		binary.LittleEndian.PutUint32(num, uint32(elem))
		val := chunk.Get(num)

		assert.True(bytes.Equal(val, randomStrings[elem]))
	}
	assert.True(chunk.IsFull())
	assert.Equal(uint32(totalValueSize), chunk.nextFreeByte)

	// check that the direct hashes match
	for i := int32(0); i < int32(chunkSize); i++ {
		h := sha256.New()
		key := chunk.getKey(i)
		h.Write(key)
		val := chunk.Get(key)
		h.Write(val)
		offset := chunk.maxSize - 1
		assert.True(bytes.Equal(chunk.hashes[i+offset], h.Sum(nil)))
	}

	// check that the inner hashes match
	for i := 0; i < chunkSize-1; i++ {
		parentH := chunk.hashes[i]
		h := sha256.New()
		h.Write(chunk.hashes[leftChildOffset(int32(i), int32(chunk.root))])
		h.Write(chunk.hashes[rightChildOffset(int32(i), int32(chunk.root))])

		assert.True(bytes.Equal(parentH, h.Sum(nil)))
	}

}

// helper function: Generate a random string of bytes to simulate random valuesint32(
func getRandomString(size int) []byte {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ")
	str := make([]byte, size)
	for i := 0; i < size; i++ {
		j := rand.Intn(len(letterRunes))
		str[i] = byte(letterRunes[j])
	}
	return str
}

// func TestMakeHeap(t *testing.T) {
// 	assert := assert.New(t)

// 	size := int16(1024)
// 	chunk := NewHeapChunk(size)

// 	assert.True(chunk.currSize == int16(0))
// 	assert.True(chunk.maxSize == size)
// 	assert.True(len(chunk.keys) == int(size))
// 	assert.True(len(chunk.values) == int(size))
// 	assert.True(len(chunk.hashes) == int(2*size-1))

// 	rand.Seed(time.Now().UnixNano())
// 	x := rand.Perm(int(size))
// 	for i, elem := range x {
// 		num := make([]byte, 4)
// 		binary.LittleEndian.PutUint32(num, uint32(elem))
// 		chunk.Insert(num, num)

// 		assert.Equal(chunk.currSize, int16(i+1))
// 		//fmt.Println(chunk.hashes[chunk.root])
// 		//chunk.computeHashes()
// 		//fmt.Println(chunk.hashes[chunk.root])
// 	}
// 	assert.True(chunk.IsFull())

// 	//fmt.Println(chunk.hashes[chunk.root])
// 	//chunk.computeHashes()
// 	//fmt.Println(chunk.hashes[chunk.root])

// 	// check that the direct hashes match
// 	for i := int16(0); i < size; i++ {
// 		h := sha256.New()
// 		h.Write(chunk.keys[i])
// 		h.Write(chunk.values[i])
// 		offset := chunk.maxSize - 1
// 		assert.True(bytes.Equal(chunk.hashes[i+offset], h.Sum(nil)))
// 	}

// 	// check that the inner hashes match
// 	for i := int16(0); i < size-int16(1); i++ {
// 		parentH := chunk.hashes[i]
// 		h := sha256.New()
// 		h.Write(chunk.hashes[leftChildOffset(int(i), int(chunk.root))])
// 		h.Write(chunk.hashes[rightChildOffset(int(i), int(chunk.root))])

// 		assert.True(bytes.Equal(parentH, h.Sum(nil)))
// 		//fmt.Println(i)
// 	}
// 	assert.NotNil(chunk)

// }

// //
func TestSplitSmallHeapInsertLeft(t *testing.T) {
	assert := assert.New(t)

	chunkSize := int16(8)
	chunk := NewHeapChunk(int32(16000000), int32(1024), int32(4), int32(chunkSize))

	chunk.Insert([]byte{10, 10, 10, 10}, []byte("Dieci"))
	chunk.Insert([]byte{70, 10, 10, 10}, []byte("Settanta"))
	chunk.Insert([]byte{80, 10, 10, 10}, []byte("Punk Rock"))
	chunk.Insert([]byte{90, 10, 10, 10}, []byte("Novanta"))
	chunk.Insert([]byte{40, 10, 10, 10}, []byte("Ciao Ciao Ciao"))
	chunk.Insert([]byte{60, 10, 10, 10}, []byte("Abcdefgh"))
	chunk.Insert([]byte{50, 10, 10, 10}, []byte("cento diviso due"))
	chunk.Insert([]byte{20, 10, 10, 10}, []byte("Some Text"))

	assert.True(chunk.IsFull())

	left, midKey, right := chunk.InsertAndSplit([]byte{30, 10, 10, 10}, []byte("New value that goes in the left chunk"))

	// check that the right chunk contains the correct values
	val := right.Get([]byte{60, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Abcdefgh"), val))

	val = right.Get([]byte{70, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Settanta"), val))

	val = right.Get([]byte{80, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Punk Rock"), val))

	val = right.Get([]byte{90, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Novanta"), val))

	assert.Equal(int32(4), right.currKeysNumber)

	// check that the left chunk contains the correct values
	val = left.Get([]byte{30, 10, 10, 10})
	assert.True(bytes.Equal([]byte("New value that goes in the left chunk"), val))

	val = left.Get([]byte{10, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Dieci"), val))

	val = left.Get([]byte{20, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Some Text"), val))

	val = left.Get([]byte{40, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Ciao Ciao Ciao"), val))

	val = left.Get([]byte{50, 10, 10, 10})
	assert.True(bytes.Equal([]byte("cento diviso due"), val))

	assert.Equal(int32(5), left.currKeysNumber)
	assert.Equal(int32(4), right.currKeysNumber)

	assert.True(bytes.Equal(midKey, right.getKey(0)))

}

func TestSplitSmallHeapInsertRight(t *testing.T) {
	assert := assert.New(t)

	chunkSize := int16(8)
	chunk := NewHeapChunk(int32(16000000), int32(1024), int32(4), int32(chunkSize))

	chunk.Insert([]byte{10, 10, 10, 10}, []byte("Dieci"))
	chunk.Insert([]byte{70, 10, 10, 10}, []byte("Settanta"))
	chunk.Insert([]byte{80, 10, 10, 10}, []byte("Punk Rock"))
	chunk.Insert([]byte{90, 10, 10, 10}, []byte("Novanta"))
	chunk.Insert([]byte{40, 10, 10, 10}, []byte("Ciao Ciao Ciao"))
	chunk.Insert([]byte{60, 10, 10, 10}, []byte("Abcdefgh"))
	chunk.Insert([]byte{50, 10, 10, 10}, []byte("cento diviso due"))
	chunk.Insert([]byte{20, 10, 10, 10}, []byte("Some Text"))
	assert.True(chunk.IsFull())
	left, _, right := chunk.InsertAndSplit([]byte{75, 10, 10, 10}, []byte("New value that goes in the right chunk"))

	assert.NotNil(right)

	val := right.Get([]byte{60, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Abcdefgh"), val))

	val = right.Get([]byte{70, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Settanta"), val))

	val = right.Get([]byte{75, 10, 10, 10})
	assert.True(bytes.Equal([]byte("New value that goes in the right chunk"), val))

	val = right.Get([]byte{80, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Punk Rock"), val))

	val = right.Get([]byte{90, 10, 10, 10})
	assert.True(bytes.Equal([]byte("Novanta"), val))

	assert.Equal(int32(5), right.currKeysNumber)
	assert.Equal(int32(4), left.currKeysNumber)
}

// func TestSplitHeap(t *testing.T) {
// 	assert := assert.New(t)

// chunkSize := int16(64)

// chunk := NewHeapChunk(int32(16000000), int32(1024), int32(4), int32(chunkSize))

// rand.Seed(time.Now().UnixNano())
// x := rand.Perm(int(chunkSize + 1))

// // Insert all elements but the last (fill the chunk but dont split yet)
// for i := int16(0); i < chunkSize; i++ {
// 	elem := x[i]
// 	num := make([]byte, 4)
// 	binary.LittleEndian.PutUint32(num, uint32(elem))
// 	chunk.Insert(num, num)
// }

// assert.True(chunk.isFull())

// // cause a SPLIT by inserting the last element
// num := make([]byte, 4)
// binary.LittleEndian.PutUint32(num, uint32(x[chunkSize]))
// left, midKey, right := chunk.InsertAndSplit(num, num)
// assert.NotNil(midKey)
// assert.NotNil(left)
// assert.NotNil(right)

// 	assert.True(left.currSize+right.currSize == chunk.maxSize+1)
// 	assert.NotNil(midKey)

// 	// check that the direct hashes (second half) match
// 	for i := int16(0); i < size; i++ {
// 		h := sha256.New()
// 		h.Write(left.keys[i])
// 		h.Write(left.values[i])
// 		offset := left.maxSize - 1
// 		assert.True(bytes.Equal(left.hashes[i+offset], h.Sum(nil)))
// 	}
// 	for i := int16(0); i < size; i++ {
// 		h := sha256.New()
// 		h.Write(right.keys[i])
// 		h.Write(right.values[i])
// 		offset := right.maxSize - 1
// 		assert.True(bytes.Equal(right.hashes[i+offset], h.Sum(nil)))
// 	}

// 	// check that the inner hashes (first half) match
// 	for i := left.root; i < size-int16(1); i++ {
// 		parentH := left.hashes[i]
// 		h := sha256.New()
// 		h.Write(left.hashes[leftChildOffset(int(i), int(left.root))])
// 		h.Write(left.hashes[rightChildOffset(int(i), int(left.root))])
// 		assert.True(bytes.Equal(parentH, h.Sum(nil)))
// 	}
// 	for i := right.root; i < size-int16(1); i++ {
// 		parentH := right.hashes[i]
// 		h := sha256.New()
// 		h.Write(right.hashes[leftChildOffset(int(i), int(right.root))])
// 		h.Write(right.hashes[rightChildOffset(int(i), int(right.root))])
// 		assert.True(bytes.Equal(parentH, h.Sum(nil)))
// 	}

// 	assert.True(bytes.Equal(midKey, right.keys[0]))

// 	// check that re-hashing does not change the hash value in the root
// 	oldRootHash := make([]byte, 32)

// 	copy(oldRootHash, left.hashes[left.root])
// 	left.computeHashes()
// 	assert.True(bytes.Equal(left.hashes[left.root], oldRootHash))

// copy(oldRootHash, right.hashes[right.root])
// right.computeHashes()
// assert.True(bytes.Equal(right.hashes[right.root], oldRootHash))
// }
