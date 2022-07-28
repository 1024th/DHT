package kademlia

import (
	"crypto/sha1"
)

func Hash(s string) (result IDType) {
	hash := sha1.New()
	hash.Write([]byte(s))
	tmp := hash.Sum(nil)
	copy(result[:], tmp)
	return
}

func Xor(a IDType, b IDType) (result IDType) {
	for i := 0; i < IDLength; i++ {
		result[i] = a[i] ^ b[i]
	}
	return
}

func PrefixLen(id IDType) int {
	for i := 0; i < IDLength; i++ {
		for j := 0; j <= 7; j++ {
			if (id[i]>>(7-j))&(0b1) != 0 {
				return (i << 3) + j
			}
		}
	}
	return (IDLength << 3) - 1
}

func (a IDType) LessThan(b IDType) bool {
	for i := 0; i < IDLength; i++ {
		if a[i] < b[i] {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}
	return false
}
