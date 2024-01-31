package chunk

import (
	"bytes"
	"crypto/sha256"
	"math"
)

// "encoding/binary"

// HeapChunk represents chunked data in the form of a heap. It consists of slices of n keys and n values together with
// a heap representation of those values with their hashes. This means that there is a slice of hashes which is 2*n-1
// in size. The first (n-1) hashes are the inner nodes in the heap while the last n hashes are the direct hashes
// of the K-V pairs. These are the leaves on the (merkle) heap tree.
// Remember that to find the direct hash of an element (a K-V pair) found at index i,
// one must add the offset of n-1 in HeapChunk.hashes.
// Keys needs to have a fixed size while values can have a variable size.
// The keys array has a fixed size and capacity -> use built in COPY function.
// The values array has a guessed capacity and 0 size -> use built in APPEND function.
type HeapChunk struct {
	keys   []byte // a flat array representing the fixed-sized keys
	values []byte // flat array representing the values

	// the hashes represent the heap itself: it is 2*maxSize -1 in size.
	// The first half - 1 hash-values represent the inner nodes.
	// The last half represents the direct hash-values of the keys and values.
	// This means that there is an offset of maxSize - 1 to the index of k-v pair and the index of their direct hash.
	hashes [][]byte

	// metadata
	currKeysNumber     int32
	root               int32
	keySize            int32 // the size of a single keys in bytes
	keyAndMetadataSize int32
	maxSize            int32  // maximal number of keys that a chunk can contain
	nextFreeByte       uint32 // represents the next free byte in the values

	indexBytes int32 // how many bytes are appended to the key to state the position of the data
	sizeBytes  int32 // how many bytes are appended to the key to state the length of the data
}

// return the number of bytes that can represent an integer able to address every
// byte in the maximal size of the chunk
// func addressingBytes(maxChunkSize int) {
// 	math.Log2()
// }

func (chunk *HeapChunk) isLeaf(i int32) bool {
	return i >= chunk.maxSize-1
}

// // FIXME
func (chunk *HeapChunk) GetCurrSize() int32 {
	return chunk.currKeysNumber
}

func (chunk *HeapChunk) GetHash() []byte {
	return chunk.hashes[chunk.root] // return the root of the heap
}

// GetSmallestKey returns the slice where the smallest key is found.
// Note that this is NOT A COPIED slice. If the content of HeapChunk.keys
// in the first HeapChunk.keySize bytes changes, that change is reflected in the returned value.
func (chunk *HeapChunk) GetSmallestKey() []byte {
	return chunk.keys[0:chunk.keySize]
}

func NewHeapChunk(maxCapacity, maxValueSize, keySize, maxSize int32) *HeapChunk {

	indexBytes := math.Ceil(math.Log2(float64(maxCapacity)) / 8)
	sizeBytes := math.Ceil(math.Log2(float64(maxValueSize)) / 8)
	if sizeBytes > indexBytes {
		panic("Single element size > Maximal capacity")
	}

	// guess total value size to be around the same of the keys (for sure a lowerbound)
	guessMaxSize := maxSize * keySize
	keyAndMetadataSize := keySize + int32(indexBytes+sizeBytes)
	chunk := &HeapChunk{
		keys:               make([]byte, maxSize*keyAndMetadataSize),
		values:             make([]byte, 0, guessMaxSize),
		hashes:             make([][]byte, (2*maxSize)-1),
		indexBytes:         int32(indexBytes),
		sizeBytes:          int32(sizeBytes),
		keySize:            keySize,
		maxSize:            maxSize,
		nextFreeByte:       0,
		keyAndMetadataSize: keyAndMetadataSize,
	}

	// offset := maxSize - 1
	//add the hashes of (nil,nil) for a correct usage of the heap.
	// for i := offset; i < offset+maxSize; i++ {
	// 	// chunk.hashes[i] = getNilHash()
	// }
	return chunk
}

// returns a new empty HeapChunk with same carachteristics as the input one
func NewHeapChunkCopy(otherChunk *HeapChunk) *HeapChunk {
	newChunk := &HeapChunk{
		keys:               make([]byte, len(otherChunk.keys)),
		values:             make([]byte, 0, otherChunk.maxSize*otherChunk.keyAndMetadataSize), // guess a value size (lowerbound)
		hashes:             make([][]byte, (2*otherChunk.maxSize)-1),
		indexBytes:         otherChunk.indexBytes,
		sizeBytes:          otherChunk.sizeBytes,
		keySize:            otherChunk.keySize,
		maxSize:            otherChunk.maxSize,
		nextFreeByte:       0,
		keyAndMetadataSize: otherChunk.keyAndMetadataSize,
	}
	// offset := newChunk.maxSize - 1

	// //add the hashes of (nil,nil) for a correct usage of the heap.
	// for i := offset; i < offset+newChunk.maxSize; i++ {
	// 	// newChunk.hashes[i] = getNilHash()
	// }
	return newChunk
}

func (chunk *HeapChunk) IsFull() bool {
	return chunk.currKeysNumber >= chunk.maxSize
}

func (chunk *HeapChunk) getOffset() int32 {
	return chunk.maxSize - 1
}

// Given the index of a key, get the corresponding byte where the key starts within the keys array
func (chunk *HeapChunk) indexToByte(keyIndex int32) int32 {
	return keyIndex * chunk.keyAndMetadataSize
}

// Given the index of a key, get the key values, excluding metadata
func (chunk *HeapChunk) getKey(keyIndex int32) []byte {
	b := chunk.indexToByte(keyIndex)
	return chunk.keys[b : b+int32(chunk.keySize)]
}

// setNewValueStartIndex is used when values are moved around in the chunk (eg splits). The value's starting index (starting byte in the values array)
// that are encoded in the key metadata must be updated to its new position.
// The caller must pass the key's index (eg the i-th key in the chunk) and the new value.
func (chunk *HeapChunk) setNewValueStartIndex(keyIndex int32, index uint32) {
	b := keyIndex * chunk.keyAndMetadataSize
	LittleEndianEncodeUint(chunk.keys[b+chunk.keySize:b+chunk.keySize+chunk.indexBytes], index)
	//LittleEndianEncodeUint(chunk.keys[b+chunk.keyAndMetadataSize-chunk.sizeBytes:b+chunk.keyAndMetadataSize], size)
}

// getValueStartIndex returns the first byte where a value resides within the values array.
// The called must pass the index to the key mapped to the desired value.
func (chunk *HeapChunk) getValueStartIndex(keyIndex int32) uint32 {
	b := chunk.indexToByte(keyIndex)
	start := b + chunk.keySize
	end := b + chunk.keySize + chunk.indexBytes
	return LittleEndianDecodeUint32(chunk.keys[start:end])
}

// getValueLength returns the length of the value that is mapped to the given key.
// The called must give the index to the desired key.
func (chunk *HeapChunk) getValueLength(keyIndex int32) uint32 {
	b := chunk.indexToByte(keyIndex)
	start := b + chunk.keySize + chunk.indexBytes
	end := b + chunk.keySize + chunk.indexBytes + chunk.sizeBytes
	return LittleEndianDecodeUint32(chunk.keys[start:end])
}

func (chunk *HeapChunk) getInsertionIndex(key []byte) int32 {
	l, r := int32(0), chunk.currKeysNumber
	var compared int

	for l < r {
		if l == r-1 {
			compared = bytes.Compare(key, chunk.getKey(l))
			if compared == 0 || compared == 1 {
				return l + 1
			}
			return l
		}
		m := (l + r) / 2
		compared = bytes.Compare(key, chunk.getKey(m))

		if compared == 1 {
			l = m
		} else if compared == -1 {
			r = m
		} else {
			return m + 1
		}
	}
	if chunk.currKeysNumber == 0 {
		return 0
	}
	// should not be reached
	return 0
}

func encodeIndexAndLength(key []byte, index, length uint32, indexBufLength, lengthBufLength int32) []byte {
	indexBytes := make([]byte, indexBufLength)
	lengthBytes := make([]byte, lengthBufLength)

	LittleEndianEncodeUint(indexBytes, index)
	LittleEndianEncodeUint(lengthBytes, length)

	key = append(key, indexBytes...)
	return append(key, lengthBytes...)
}

// LittleEndianEncodeUint take an unsigned integer and a buffer.
// The function only encodes so many bytes as the buffer has. At most it can have 4 bytes.
// It works similarly to binary.LittleEndiand.PutUint32 but accepts variable sized buffers.
func LittleEndianEncodeUint(buff []byte, num uint32) {
	for i := 0; i < len(buff); i++ {
		buff[i] = byte(num >> (i * 8))
	}
}

// LittleEndianDecodeUint32 takes a buffer of size 1 to 4 bytes and decodes the uinsigned integer contained
// It works similarly to binary.LittleEndiand.Uint32 but accepts variable sized buffers.
func LittleEndianDecodeUint32(buff []byte) uint32 {
	var x uint32 = 0
	for i := 0; i < len(buff); i++ {
		x = x | uint32(buff[i])<<(i*8)
	}
	return x
}

func (chunk *HeapChunk) Insert(key, value []byte) {
	if chunk.IsFull() {
		panic("Inserting a full chunk")
	}
	h := sha256.New()

	insertionIndex := chunk.getInsertionIndex(key)
	j := chunk.currKeysNumber
	offset := chunk.getOffset() // the hash of an element is shifted left by the number of inner nodes in the heap
	// make space in the hashes
	for j > insertionIndex {
		// chunk.keys[j] = chunk.keys[j-1]
		// chunk.values[j] = chunk.values[j-1]
		chunk.hashes[j+offset] = chunk.hashes[(j+offset)-1]
		j -= 1
	}
	// make space in the keys
	firstKeyByte := chunk.indexToByte(insertionIndex)
	lastKeyByte := chunk.currKeysNumber * chunk.keyAndMetadataSize

	copy(chunk.keys[firstKeyByte+chunk.keyAndMetadataSize:lastKeyByte+chunk.keyAndMetadataSize], chunk.keys[firstKeyByte:lastKeyByte])

	// Insert new key
	encodedKey := encodeIndexAndLength(key, chunk.nextFreeByte, uint32(len(value)), chunk.indexBytes, chunk.sizeBytes)
	copy(chunk.keys[firstKeyByte:firstKeyByte+chunk.keyAndMetadataSize], encodedKey)
	// TODO insert new value
	chunk.values = append(chunk.values, value...)
	chunk.nextFreeByte += uint32(len(value))

	h.Write(key)
	h.Write(value)

	chunk.hashes[insertionIndex+offset] = h.Sum(nil)
	chunk.currKeysNumber += 1
	if chunk.currKeysNumber%2 == 0 {
		chunk.root = offset - (chunk.currKeysNumber - 1)
	} else {
		chunk.root = offset - chunk.currKeysNumber
	}
	chunk.computeHashes()

}

func (chunk *HeapChunk) computeRootPosition() {
	offset := chunk.maxSize - int32(1)

	if chunk.currKeysNumber%2 == 0 {
		chunk.root = offset - (chunk.currKeysNumber - 1)
	} else {
		chunk.root = offset - chunk.currKeysNumber
	}
}

// compactValues closes holes in the values array caused by a split to put all values in contiguous memory.
// It requires that currKeysNumber is up-to-date.
func (chunk *HeapChunk) compactValues() {
	valueSizeGuess := chunk.maxSize * chunk.keyAndMetadataSize
	leftCompactValues := make([]byte, 0, valueSizeGuess)
	bytesCount := uint32(0)
	for k := int32(0); k < chunk.currKeysNumber; k++ {
		startValue := chunk.getValueStartIndex(k)
		lenValue := chunk.getValueLength(k)
		leftCompactValues = append(leftCompactValues, chunk.values[startValue:startValue+lenValue]...)
		chunk.setNewValueStartIndex(k, bytesCount)
		bytesCount += lenValue
	}

	chunk.values = leftCompactValues
	chunk.nextFreeByte = bytesCount
}

// FIXME NOW KEYS ARE IN A DYNAMIC ARRAY. RETURN A COPY, NOT A POINTER! (I guess?)
func (chunk *HeapChunk) InsertAndSplit(key, value []byte) (leftChunk *HeapChunk, middleKey []byte, rightChunk *HeapChunk) {
	if !chunk.IsFull() {
		panic("Chunk not full")
	}
	rightChunk = NewHeapChunkCopy(chunk)

	offset := chunk.getOffset()

	insertionIndex := chunk.getInsertionIndex(key)
	m_1 := (chunk.currKeysNumber + 1) / 2

	i := m_1
	j := 0
	if insertionIndex < m_1 { // new value is in the LEFT CHUNK
		// copy right part in the right chunk
		changedSize := chunk.currKeysNumber - i

		// move the keys
		firstKeyByte := chunk.indexToByte(i)
		lastKeyByte := chunk.currKeysNumber * chunk.keyAndMetadataSize

		copy(rightChunk.keys[0:], chunk.keys[firstKeyByte:lastKeyByte])
		// TODO empty keys beyond the mid in right (=set to 0)? Or keep junk??

		for i != chunk.currKeysNumber {
			rightChunk.hashes[int32(j)+offset] = chunk.hashes[i+offset]

			// chunk.hashes[i+offset] = getNilHash()
			chunk.hashes[i+offset] = nil
			i += 1
			j += 1
		}
		rightChunk.currKeysNumber = changedSize

		// MOVE values to the RIGHT CHUNK
		for k := int32(0); k < rightChunk.currKeysNumber; k++ {
			startValue := rightChunk.getValueStartIndex(k)
			lenValue := rightChunk.getValueLength(k)
			rightChunk.values = append(rightChunk.values, chunk.values[startValue:startValue+lenValue]...)
			rightChunk.setNewValueStartIndex(k, rightChunk.nextFreeByte)
			rightChunk.nextFreeByte += lenValue
		}
		rightChunk.computeRootPosition()
		rightChunk.computeHashes()

		chunk.currKeysNumber -= changedSize
		// compact values (close holes)
		chunk.compactValues()

		// chunk.computeRootPosition()
		chunk.Insert(key, value) // this also updates root index

		// !!NOTE!!:
		// Returning the middle key like this (without making a copy) assumes that the underlying array of keys is never changed (reallocated)
		// This means that whatever happens to the chunk (eg splits), the left-most key is always in the first 'keyAndMetadataSize' positions.
		// Only keys that are not the left-most are affected by splits with the current implementation.
		// Keep this in mind if you want to change this structure.
		return chunk, rightChunk.keys[0:rightChunk.keySize], rightChunk

	}
	// new value is in the RIGHT CHUNK
	changedSize := chunk.currKeysNumber - i

	h := sha256.New()
	h.Write(key)
	h.Write(value)
	// make space and insert new hash value and values
	// also move values (new values is not yet appended!)
	for i != chunk.currKeysNumber {
		if insertionIndex == m_1+int32(j) { // Insert the new value in the right chunk
			rightChunk.hashes[int32(j)+offset] = h.Sum(nil)
			j += 1
		} else { // copy the right half of the chunk's hash values
			startValue := chunk.getValueStartIndex(i)
			lenValue := chunk.getValueLength(i)
			rightChunk.values = append(rightChunk.values, chunk.values[startValue:startValue+lenValue]...)
			// the metadata (startIndex) in the keys is already updated while the keys are still in the old chunk.
			// When keys are copied to the right side, they dont need any update (thats why index i is used)
			chunk.setNewValueStartIndex(i, rightChunk.nextFreeByte)
			rightChunk.nextFreeByte += lenValue

			rightChunk.hashes[int32(j)+offset] = chunk.hashes[i+offset]
			// chunk.hashes[i+offset] = getNilHash()
			chunk.hashes[i+offset] = nil
			i += 1
			j += 1
		}
	}
	// edge case: the value is inserted at the very end (not covered in the loop above)
	if insertionIndex == chunk.currKeysNumber {
		rightChunk.hashes[changedSize+offset] = h.Sum(nil)
	}
	// make space and insert new keys
	// m_1 = middle, insertionIndex >= m_1
	// CHUNK:
	// [----------(m_1)----(insert)---]
	//      |            |          |
	//   left chunk     rightLeft   rightRight
	//
	// rightLeft is the keys in the right chunk, on the left of the insertion index.
	// rightRight is the keys in the right chunk, on the right of the insertion index.
	// TODO: copy rightLeft, add new key, copy leftRight

	// copy rightLeft
	firstKeyByte := chunk.indexToByte(m_1)
	lastKeyByte := chunk.indexToByte(insertionIndex)
	copy(rightChunk.keys[0:], chunk.keys[firstKeyByte:lastKeyByte])
	// copy rightRight leaving 1 spot free
	firstKeyByte = chunk.indexToByte(insertionIndex)
	copy(rightChunk.keys[((insertionIndex-m_1)+1)*chunk.keyAndMetadataSize:], chunk.keys[firstKeyByte:]) // + 1 to leave space for the key to insert
	// copy new key
	encodedKey := encodeIndexAndLength(key, rightChunk.nextFreeByte, uint32(len(value)), rightChunk.indexBytes, rightChunk.sizeBytes)
	copy(rightChunk.keys[(insertionIndex-m_1)*chunk.keyAndMetadataSize:((insertionIndex-m_1)+1)*chunk.keyAndMetadataSize], encodedKey)
	// copy new value
	rightChunk.values = append(rightChunk.values, value...)
	rightChunk.nextFreeByte += uint32(len(value))

	rightChunk.currKeysNumber = changedSize
	rightChunk.currKeysNumber += 1
	rightChunk.computeRootPosition()
	rightChunk.computeHashes()
	chunk.currKeysNumber -= changedSize
	chunk.compactValues()
	chunk.computeRootPosition()
	chunk.computeHashes()
	return chunk, rightChunk.keys[0:rightChunk.keySize], rightChunk

}

// given the index i in HeapChunk.hashes, returns the index of its left child.
// The caller must check that the returned index is within the range of the heap.
func leftChild(i int) int {
	return 2*i + 1
}

// leftChildOffset works like leftChild, but assuming a heap-root not found at index 0.
// HeapChunk.root is passed as an extra argument.
func leftChildOffset(i, offset int32) int32 {
	return (2*i + 1) - offset
}

// given the index i in HeapChunk.hashes, returns the index of its right child.
// The caller must check that the returned index is within the range of the heap.
func rightChild(i int) int {
	return 2*i + 2
}

// rightChildOffset works like rightChild, but assuming a heap-root not found at index 0.
// HeapChunk.root is passed as an extra argument.
func rightChildOffset(i, offset int32) int32 {
	return (2*i + 2) - offset
}

// // parent returns the index of the parent node of a given index in the heap.
// func parent(i int) int {
// 	x := math.Floor((float64(i) - 1.0) / 2.0)
// 	return int(x)
// }

// parentOffset works like parent but assuming the heap-root to not be found at index 0.
// HeapChunk.root is passed as an extra argument.
func parentOffset(i, offset int) int {
	x := math.Floor((float64(i) + float64(offset) - 1.0) / 2.0)
	return int(x)
}

// sibling returns the index of the sibling of a given index, in the tree representation of a heap.
// It also returns a boolean value set to True when the sibling is on the left of the given index.
func sibling(i int) (int, bool) {
	if i == 0 {
		return -1, false
	}
	if i%2 == 0 {
		return i - 1, true
	} else {
		return i + 1, false
	}

}

func getNilHash() []byte {
	// h := sha256.New()
	// h.Write(nil)
	// h.Write(nil)
	// return h.Sum(nil)
	return nil
}

// computeHashes updates the hashes of the inner hash-values (the first half) in HeapChunk.hashes.
// It is assumed that a new K-V pair was inserted,
// and the direct hash-value of the pair was computed and inserted (in the second half).
func (chunk *HeapChunk) computeHashes() {
	chunk.computeHashesHelper(chunk.root)
}

func (chunk *HeapChunk) computeHashesHelper(i int32) []byte {
	// if it's a leaf, use the direct hash of the K-V pair.
	if chunk.isLeaf(i) {
		return chunk.hashes[i]
	}

	// recursively compute the hash of the current index by obtaining the hash value of the left and right child.
	left := chunk.computeHashesHelper(leftChildOffset(i, chunk.root))
	right := chunk.computeHashesHelper(rightChildOffset(i, chunk.root))

	// update the current index
	h := sha256.New()
	h.Write(left)
	h.Write(right)
	hash := h.Sum(nil)
	chunk.hashes[i] = hash

	return hash
}

func (chunk *HeapChunk) Get(key []byte) []byte {
	size := chunk.currKeysNumber
	l, r := int32(0), size
	for l < r {
		m := (l + r) / 2
		compared := bytes.Compare(key, chunk.getKey(m))
		if compared == 0 {
			start := chunk.getValueStartIndex(m)
			end := chunk.getValueLength(m)
			return chunk.values[start : start+end]
		}
		if compared == -1 {
			r = m
		} else {
			l = m + 1
		}
	}
	return nil
}

func (chunk *HeapChunk) indexOf(key []byte) int32 {
	size := chunk.currKeysNumber
	l, r := int32(0), size
	for l < r {
		m := (l + r) / 2
		compared := bytes.Compare(key, chunk.getKey(m))
		if compared == 0 {
			return m
		}
		if compared == -1 {
			r = m
		} else {
			l = m + 1
		}
	}
	return -1
}

// CorruptData negates the bits of the first byte in the data.
// Used for testing only.
func (chunk *HeapChunk) CorruptData(i int) {
	chunk.values[i] = ^chunk.values[i]
}
