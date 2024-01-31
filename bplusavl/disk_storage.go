package bplusavl

import (
	hchunk "bplus/chunk"
	"io"

	"github.com/pkg/errors"
	"github.com/tendermint/go-amino"
)

func (node *Node) Serialize(buffer io.Writer) error {
	if !node.isLeaf() {
		panic("Trying to serialize a non-leaf node")
	}

	// serialize all METADATA
	err := amino.EncodeUint32(buffer, node.leafID) // Leaf ID
	if err != nil {
		return errors.Wrap(err, "while encoding leaf-id")
	}
	err = amino.EncodeUint8(buffer, node.keyHeight) // key Height
	if err != nil {
		return errors.Wrap(err, "while encoding key Height")
	}

	err = node.chunk.Serialize(buffer)
	if err != nil {
		return err
	}
	return nil

}

func Deserialize(buffer []byte, maxSize int32) (*Node, error) {

	leafID, j, err := amino.DecodeUint32(buffer)
	if err != nil {
		return nil, err
	}
	buffer = buffer[j:]

	var keyHeight uint8
	keyHeight, j, err = amino.DecodeUint8(buffer)
	if err != nil {
		return nil, err
	}
	buffer = buffer[j:]

	var chunk *hchunk.HeapChunk
	chunk, err = hchunk.Deserialize(buffer, maxSize)
	if err != nil {
		return nil, err
	}

	leaf := &Node{
		chunk:     chunk,
		leafID:    leafID,
		keyHeight: keyHeight,
	}
	leaf.calcHash()
	leaf.hashIsValid = true

	return leaf, nil
}
