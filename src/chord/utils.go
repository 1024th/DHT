package chord

import (
	"crypto/sha1"
	"math/big"
	"net"
	"net/rpc"
	"time"
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

func hashAdd(x *big.Int, y int) *big.Int {
	return new(big.Int).And(new(big.Int).Add(x, big.NewInt(int64(y))), hashMask)
}

var (
	timeout time.Duration = 400 * time.Millisecond
)

func GetClient(addr string) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	client := rpc.NewClient(conn)
	return client, err
}

func RemoteCall(addr string, serviceMethod string, args interface{}, reply interface{}) error {
	client, err := GetClient(addr)
	if err != nil {
		return err
	}
	if client != nil {
		defer client.Close()
	}
	err2 := client.Call(serviceMethod, args, reply)
	return err2
}
