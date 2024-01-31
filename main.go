package main

import (
	"bplus/bplusavl"
	"fmt"
)

func main() {

	tree := bplusavl.NewIAVL(int32(4), int32(1))

	keys := [][]byte{{10}, {50}, {30}, {40}, {60}, {20}, {70}, {100}, {80}, {90}}
	values := [][]byte{[]byte("Hello"),
		[]byte("World!"),
		[]byte("This"),
		[]byte("Is"),
		[]byte("A"),
		[]byte("B+Tree"),
		[]byte("containing"),
		[]byte("some"),
		[]byte("random"),
		[]byte("values")}

	for i := 0; i < len(keys); i++ {
		tree.Set(keys[i], values[i])
	}

	elemProof, _ := tree.GetElementProof([]byte{20})

	proof_hash := elemProof.ValidateProof([]byte{20}, []byte("B+Tree"))
	root_hash := tree.GetRootHash()
	fmt.Println("Proof hash:", proof_hash)
	fmt.Println("Root hash:", root_hash)
}
