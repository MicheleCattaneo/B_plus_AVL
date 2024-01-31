package bplusavl

import (
	hchunk "bplus/chunk"
	"bytes"
	"crypto/sha256"
	"io"

	"github.com/pkg/errors"
	"github.com/tendermint/go-amino"
)

// IAVLLeafProof is a proof composed of a path from the root node of the tree, down to a leaf node.
type IAVLLeafProof struct {
	hashes     [][]byte
	directions []bool
}

// IAVLElementProof is a proof composed of a path from the root node of the tree, down to a single K-V pair
// in a chunk.
type IAVLElementProof struct {
	iavlProof  *IAVLLeafProof         // the path traversing the tree down to a leaf
	chunkProof *hchunk.HeapChunkProof // the path traversing the chunk
	keyHeight  uint8
}

func (proof *IAVLLeafProof) GetLength() int {
	return len(proof.hashes)
}

func (proof *IAVLElementProof) GetLength() int {
	return proof.iavlProof.GetLength() + proof.chunkProof.GetLength()
}

// GetElementProof returns a proof for a given key in the tree.
// The proof can be validated by IAVLElementProof.ValidateProof, which returns a hash value that should match
// the hash value found at the root node of the tree, if the key is found in the tree.
func (tree *IAVL) GetElementProof(key []byte) (*IAVLElementProof, error) {
	leafProof, leaf, err1 := tree.getLeafProof(key)
	chunkProof, err2 := leaf.chunk.GetProof(key)

	if err1 != nil || err2 != nil {
		return nil, errors.New("Error by creating a proof")
	}
	return &IAVLElementProof{
		leafProof,
		chunkProof,
		leaf.keyHeight,
	}, nil
}

// ValidateProof validates the proof for a given K-V pair. If the proof was valid, the returned hash value
// should match the root hash in the tree that generated the proof.
func (proof *IAVLElementProof) ValidateProof(key, value []byte) []byte {
	// obtain the hash from the root hash in the chunk (heap)
	chunkHash := proof.chunkProof.ValidateProof(key, value)

	// add keyHeight to the hash
	h := sha256.New()
	h.Write([]byte{proof.keyHeight}) // add keyHeight to the hash
	h.Write(chunkHash)

	leafHash := h.Sum(nil)

	// obtain the hash from the root hash in the whole tree, reusing the hash obtained from the chunk
	rootHash := proof.iavlProof.ValidateProof(leafHash)
	return rootHash
}

// getChunkProof returns the proof for the chunk (=leaf containing a chunk) at a certain position in the tree,
// where 0 is the left-most chunk and C = nextLeafID - 1 is the right-most chunk.
// Remember that IDs are given incrementally and due to splits, chunks are not sorted by ID.
func (tree *IAVL) GetChunkProof(chunkPosition int) (*IAVLLeafProof, *Node, error) {
	leaf := tree.chunkList.GetChunk(chunkPosition)
	if leaf == nil {
		return nil, nil, errors.New("Chunk not found in the chunkList")
	}
	k := leaf.chunk.GetSmallestKey()
	return tree.getLeafProof(k)
}

func (tree *IAVL) getLeafProof(key []byte) (*IAVLLeafProof, *Node, error) {
	var hashes [][]byte
	var directions []bool

	currNode := tree.root
	for !currNode.isLeaf() {
		if bytes.Compare(key, currNode.key) == -1 {
			// path goes on the left, add RIGHT sibling to proof
			hashes = append([][]byte{currNode.rightNode.hash}, hashes...)
			directions = append([]bool{false}, directions...)
			currNode = currNode.leftNode
		} else {
			// path goes on the right, add LEFT sibling to proof
			hashes = append([][]byte{currNode.leftNode.hash}, hashes...)
			directions = append([]bool{true}, directions...)
			currNode = currNode.rightNode
		}
	}
	return &IAVLLeafProof{
		hashes:     hashes,
		directions: directions,
	}, currNode, nil
}

// ValidateProof called on a proof and given the hash of the leaf node (a chunk),
// returns the hash that should match the root hash. In case of match, the chunk can be
// considered valid.
func (proof *IAVLLeafProof) ValidateProof(leafHash []byte) []byte {
	h := sha256.New()
	currHash := leafHash
	for i := 0; i < len(proof.hashes); i++ {
		if proof.directions[i] {
			h.Write(proof.hashes[i])
			h.Write(currHash)
		} else {
			h.Write(currHash)
			h.Write(proof.hashes[i])
		}
		currHash = h.Sum(nil)
		h.Reset()
	}
	return currHash
}

// SerializeProof serializes a proof into a buffer.
func (proof *IAVLLeafProof) SerializeProof(buffer io.Writer) error {
	err := amino.EncodeInt32(buffer, int32(len(proof.hashes)))
	if err != nil {
		return errors.Wrap(err, "while encoding proof size")
	}

	for _, h := range proof.hashes {
		err = amino.EncodeByteSlice(buffer, h)
		if err != nil {
			return errors.Wrap(err, " while encoding hash")
		}
	}

	for _, dir := range proof.directions {
		err = amino.EncodeBool(buffer, dir)
		if err != nil {
			return errors.Wrap(err, "while encoding direction")
		}
	}
	return nil
}

// DeserializeProof take a buffer containing a serialized proof and rebuilds
// the proof.
func DeserializeProof(buffer []byte) (*IAVLLeafProof, error) {

	proofSize, j, err := amino.DecodeInt32(buffer)
	if err != nil {
		return nil, errors.Wrap(err, "while decoding proof size")
	}
	buffer = buffer[j:]

	hashes := make([][]byte, proofSize)
	directions := make([]bool, proofSize)

	for i := 0; i < int(proofSize); i++ {
		h, j, err := amino.DecodeByteSlice(buffer)
		if err != nil {
			return nil, errors.Wrap(err, "while decoding hash")
		}
		buffer = buffer[j:]
		hashes[i] = h
	}

	for i := 0; i < int(proofSize); i++ {
		d, j, err := amino.DecodeBool(buffer)
		if err != nil {
			return nil, errors.Wrap(err, "while decoding direction")
		}
		buffer = buffer[j:]
		directions[i] = d
	}

	return &IAVLLeafProof{hashes: hashes, directions: directions}, nil
}
