package chord

import (
	"crypto/sha1"
	"math/big"
)

// Returns (begin < target && target < end).
// If begin >= end, returns (begin < target || target < end).
func contains(target, begin, end *big.Int) bool {
	if begin.Cmp(end) < 0 {
		return begin.Cmp(target) < 0 && target.Cmp(end) < 0
	} else {
		return begin.Cmp(target) < 0 || target.Cmp(end) < 0
	}
}

/* Hash related variables and functions */

const hashLength = 160

func getHashMask() (res *big.Int) {
	one := big.NewInt(1)
	res = new(big.Int)
	res.Sub(new(big.Int).Lsh(one, hashLength), one)
	return
}

var hashMask = getHashMask()

func Hash(s string) *big.Int {
	h := sha1.New()
	h.Write([]byte(s))
	ret := new(big.Int)
	ret.SetBytes(h.Sum(nil))
	return ret
}

// Returns (x + (2**y)) % (2**M), which is the value of finger[y].start of node x.
// M is the hashLength.
func hashCalc(x *big.Int, y uint) *big.Int {
	return new(big.Int).And(new(big.Int).Add(x, new(big.Int).Lsh(big.NewInt(1), y)), hashMask)
}
