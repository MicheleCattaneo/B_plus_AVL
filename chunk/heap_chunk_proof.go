package chunk

import (
	"crypto/sha256"

	"github.com/pkg/errors"
)

// HeapChunkProof is an in-memory representation of a proof for a given element in the HeapChunk.
// It is composed by a slice of hashes and a slice of directions
// (specifying whether the hash comes from a left or a right sibling), so that the direct hash of the K-V pair
// can be processed with the proof to obtain the root hash of the HeapChunk. If the hashes match, the proof is valid.
type HeapChunkProof struct {
	hashes     [][]byte // the hash values of the siblings
	directions []bool   // directions[i] is True if the sibling is found on the left.
}

func (proof *HeapChunkProof) GetLength() int {
	return len(proof.hashes)
}

// GetProof returns a proof for the element mapped to the given key. If the key is not found in the chunk,
// an error is returned.
func (chunk *HeapChunk) GetProof(key []byte) (*HeapChunkProof, error) {
	var hashes [][]byte
	var directions []bool

	index := chunk.indexOf(key)
	if index == -1 {
		return nil, errors.New("Key not founnd")
	}
	offset := chunk.maxSize - int32(1)
	directHashIndex := int(index + offset)             // find the index for the direct hash of the K-V pair
	siblingIndex, fromLeft := sibling(directHashIndex) // find the direct hash of the sibling and add it to the proof
	hashes = append(hashes, chunk.hashes[siblingIndex])
	directions = append(directions, fromLeft)

	// until reaching the root, add the hashes of all the siblings (and their direction) to the proof
	parentIndex := parentOffset(siblingIndex, int(chunk.root))
	for parentIndex > int(chunk.root) {
		siblingIndex, fromLeft = sibling(parentIndex)
		hashes = append(hashes, chunk.hashes[siblingIndex])
		directions = append(directions, fromLeft)

		parentIndex = parentOffset(siblingIndex, int(chunk.root))
	}
	return &HeapChunkProof{
		hashes:     hashes,
		directions: directions,
	}, nil
}

// ValidateProof validate the proof for an element given the K-V pair.
// The returned value is the hash that should be found at the root of the heap.
func (proof *HeapChunkProof) ValidateProof(key, value []byte) []byte {
	h := sha256.New()
	h.Write(key)
	h.Write(value)
	currHash := h.Sum(nil) // compute hash of the K-V pair
	h.Reset()
	for i := 0; i < len(proof.hashes); i++ { // follow the proof to build-up the root hash of the heap
		if proof.directions[i] { // if it's a left sibling, add it first.
			h.Write(proof.hashes[i])
			h.Write(currHash)
		} else { // if it's a right sibling, add the current hash first.
			h.Write(currHash)
			h.Write(proof.hashes[i])
		}
		currHash = h.Sum(nil)
		h.Reset()
	}
	return currHash
}
