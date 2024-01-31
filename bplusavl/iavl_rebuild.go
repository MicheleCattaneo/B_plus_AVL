package bplusavl

import (
	"bytes"
	"sort"
)

func RebuildTree(list []*Node) *IAVL {
	n := len(list)

	activeNodes := make([]*Node, n) // fix the size

	var maxH uint8 = 0
	var root *Node = nil
	for _, node := range list {
		h := node.keyHeight
		var newNode *Node = nil
		if h != 0 {
			newNode = &Node{
				key:         node.chunk.GetSmallestKey(),
				height:      node.keyHeight,
				hashIsValid: false,
				leafPointer: node,
			}
		} else {
			newNode = node
		}
		activeNodes[h] = newNode
		// connect the active nodes, from the lowest level to the highest level.
		// This way the sizes can be computed.
		for i, j := 0, 1; j <= int(h) && i < int(h); i++ {
			if activeNodes[i] != nil {
				for activeNodes[j] == nil {
					j += 1

				}
				if activeNodes[j].leftNode == nil {
					activeNodes[j].leftNode = activeNodes[i]
				} else {
					activeNodes[j].rightNode = activeNodes[i]
				}
				activeNodes[j].size += activeNodes[i].size
				//if i != int(h) {
				activeNodes[i] = nil
				//}
				j += 1
			}
		}

		if h != 0 { // leaf nodes apart from the first one need to be set active again
			activeNodes[0] = node
		}
		// remember which one is the root and its height
		if maxH <= h {
			root = activeNodes[h]
			maxH = h
		}
	}

	// when the list is over, connect the remaining nodes
	//intMaxH := int(maxH)
	//connect(intMaxH, intMaxH, activeNodes)
	for i, j := 0, 1; j <= int(maxH); {
		for activeNodes[j] == nil {
			j += 1
		}
		activeNodes[j].rightNode = activeNodes[i]
		activeNodes[j].size += activeNodes[i].size
		i = j
		j += 1
	}

	return &IAVL{
		root:      root,
		firstLeaf: list[0],
	}
}

func SortNodeList(nodes []*Node) {
	// sort.Slice(people, func(i, j int) bool { return people[i].Name < people[j].Name })
	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i].chunk.GetSmallestKey(), nodes[j].chunk.GetSmallestKey()) == -1
	})
}
