package bplusavl

import "bytes"

// chunk list keeps a sorted list of chunks, by keeping their pointers.
// The chunks are pointer to the leaf nodes that contain the actual chunk of data.
// This structure assumes that once a chunk is inserted, its pointer will not change.
// When a split occurs, the left half needs to reutilize the same memory address, while the (new) right half needs
// to have its pointer added to this list.
// This structure only exists in main memory.
type ChunkList struct {
	chunks []*Node
}

// GetChunk returns the i-th chunk, sorted by the first key.
func (list *ChunkList) GetChunk(chunkPosition int) *Node {
	if chunkPosition < len(list.chunks) {
		return list.chunks[chunkPosition]
	}
	return nil
}

func (list *ChunkList) GetNumberOfChunks() int {
	return len(list.chunks)
}

// returns an empty chunk list. Used when a tree is created from scratch.
func NewEmptyChunkList() *ChunkList {
	return &ChunkList{}
}

// returns a chunk list with space for M chunks. Used when a tree is reconstructed.
// Read the number of chunk from block header and populate this list as chunks of data are retrieved.
func NewChunkList(size int) *ChunkList {
	return &ChunkList{
		chunks: make([]*Node, size),
	}
}

// inserts on a list that has space pre-allocated, hence when the tree is being recovered.
// Do NOT use during normal execution. This function assumes a certain preexisting size.
// Use ChunkList.appendAt() if the list should grow instead.
func (list *ChunkList) insertAt(index int, leaf *Node) {

}

func (list *ChunkList) append(leaf *Node) {
	indx := list.getInsertionIndex(leaf.chunk.GetSmallestKey())
	list.appendAt(indx, leaf)
}

// insert on a list that grows dynamically, hence during normal execution (NOT during recovery).
// Do NOT use during recovery. Use ChunkList.insertAt() instead.
func (list *ChunkList) appendAt(index int, leaf *Node) {
	if len(list.chunks) == index { // nil or empty slice or after last element
		list.chunks = append(list.chunks, leaf)
		return
	}

	// byteindex := index * keysize
	list.chunks = append(list.chunks[:index+1], list.chunks[index:]...) // index < len(a)
	// copy(a[byteindex:byteindex+keysize], value)
	list.chunks[index] = leaf
}

func (list *ChunkList) addChunk(leftMostChunkKey []byte, leaf *Node) {
	if !leaf.isLeaf() {
		panic("Can only store a leaf (= a chunck )")

	}
}

func (list *ChunkList) getInsertionIndex(leftMostChunkKey []byte) int {
	l, r := 0, len(list.chunks)
	var compared int

	for l < r {
		if l == r-1 {
			compared = bytes.Compare(leftMostChunkKey, list.chunks[l].chunk.GetSmallestKey())
			if compared == 0 || compared == 1 {
				return l + 1
			}
			return l
		}
		m := (l + r) / 2
		compared = bytes.Compare(leftMostChunkKey, list.chunks[m].chunk.GetSmallestKey())

		if compared == 1 {
			l = m
		} else if compared == -1 {
			r = m
		} else {
			return m + 1
		}
	}
	if len(list.chunks) == 0 {
		return 0
	}
	// should not be reached
	return 0
}
