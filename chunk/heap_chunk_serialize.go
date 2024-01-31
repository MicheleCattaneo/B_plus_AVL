package chunk

import (
	"crypto/sha256"
	"io"

	"github.com/pkg/errors"
	"github.com/tendermint/go-amino"
)

func (chunk *HeapChunk) Serialize(buffer io.Writer) error {

	err := amino.EncodeInt32(buffer, chunk.currKeysNumber)
	if err != nil {
		return errors.Wrap(err, "while encoding currSize")
	}

	err = amino.EncodeInt32(buffer, chunk.sizeBytes)
	if err != nil {
		return errors.Wrap(err, "while encoding sizeBytes")
	}
	err = amino.EncodeInt32(buffer, chunk.indexBytes)
	if err != nil {
		return errors.Wrap(err, "while encoding indexBytes")
	}

	err = amino.EncodeInt32(buffer, chunk.keySize)
	if err != nil {
		return errors.Wrap(err, "while encoding keySize")
	}

	// do not serialize junk data (old keys)
	err = amino.EncodeByteSlice(buffer, chunk.keys[0:chunk.keyAndMetadataSize*chunk.currKeysNumber])
	if err != nil {
		return errors.Wrap(err, "while encoding keys")
	}

	err = amino.EncodeByteSlice(buffer, chunk.values)
	if err != nil {
		return errors.Wrap(err, "while encoding values")
	}
	return nil
}

func Deserialize(buffer []byte, maxSize int32) (*HeapChunk, error) {
	// decode

	currSize, j, err := amino.DecodeInt32(buffer)
	if err != nil {
		return nil, err
	}
	buffer = buffer[j:]

	sizeBytes, j, err := amino.DecodeInt32(buffer)
	if err != nil {
		return nil, err
	}
	buffer = buffer[j:]

	indexBytes, j, err := amino.DecodeInt32(buffer)
	if err != nil {
		return nil, err
	}
	buffer = buffer[j:]

	keySize, j, err := amino.DecodeInt32(buffer)
	if err != nil {
		return nil, err
	}
	buffer = buffer[j:]

	keys, j, err := amino.DecodeByteSlice(buffer)
	// if the chunk was not full, the returned slice will have a smaller capacity.
	// The structure requires that the space for all keys is preallocated (cap(keys) == maxSize * keyAndMetadataSize)
	// Must ensure that the underlying array has the correct capacity.
	if int32(cap(keys)) < int32(maxSize)*(keySize+indexBytes+sizeBytes) {
		newKeysSlice := make([]byte, maxSize*(keySize+indexBytes+sizeBytes))
		copy(newKeysSlice[0:], keys)
		keys = newKeysSlice
	}
	if err != nil {
		return nil, err
	}
	buffer = buffer[j:]

	values, _, err := amino.DecodeByteSlice(buffer)
	if err != nil {
		return nil, err
	}

	chunk := &HeapChunk{
		hashes:             make([][]byte, (maxSize*2)-1),
		keySize:            keySize,
		keyAndMetadataSize: keySize + indexBytes + sizeBytes,
		indexBytes:         indexBytes,
		sizeBytes:          sizeBytes,
		currKeysNumber:     currSize,
		maxSize:            maxSize,
		keys:               keys,
		values:             values,
		nextFreeByte:       uint32(len(values)),
	}
	offset := maxSize - 1
	h := sha256.New()

	//add the hashes of (nil,nil) for a correct usage of the heap.
	for i := offset; i < offset+currSize; i++ {
		currKey := chunk.getKey(i - offset)
		start := chunk.getValueStartIndex(i - offset)
		end := chunk.getValueLength(i - offset)
		currVal := chunk.values[start : start+end]
		h.Write(currKey)
		h.Write(currVal)
		chunk.hashes[i] = h.Sum(nil)
		h.Reset()
	}
	// needed??
	// for i := offset + currSize; i < maxSize+offset; i++ {
	// 	chunk.hashes[i] = getNilHash()
	// }

	chunk.computeRootPosition()
	chunk.computeHashes()
	return chunk, nil
}
