package helperfunctions

import "math/rand"

// GetRandomString returns a random string of a certain size.
func GetRandomString(size int) []byte {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ")
	str := make([]byte, size)
	for i := 0; i < size; i++ {
		j := rand.Intn(len(letterRunes))
		str[i] = byte(letterRunes[j])
	}
	return str
}
