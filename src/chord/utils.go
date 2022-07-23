package chord

import (
	"crypto/sha1"
	"math/big"
	"net"
	"net/rpc"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
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

func GetClient(addr string) (*rpc.Client, error) {
	// var client *rpc.Client
	// var err error
	// for i := 0; i < 5; i++ {
	// 	// conn, err := net.DialTimeout("tcp", addr, timeout)
	// 	ch := make(chan error)
	// 	go func() {
	// 		client, err = rpc.Dial("tcp", addr)
	// 		ch <- err
	// 	}()
	// 	select {
	// 	case <-ch:
	// 		if err == nil {
	// 			return client, nil
	// 		} else {
	// 			logrus.Errorf("<GetClient> get [%s] err: %v\n", getPortFromIP(addr), err)
	// 			return nil, err
	// 		}
	// 	case <-time.After(500 * time.Millisecond):
	// 		logrus.Warnf("<GetClient> get [%s] timeout\n", getPortFromIP(addr))
	// 		err = fmt.Errorf("timeout")
	// 		continue
	// 	}
	// }
	// logrus.Errorf("<GetClient> get [%s] err: %v\n", getPortFromIP(addr), err)
	// return nil, err
	var conn net.Conn
	var err error
	for i := 0; i < 3; i++ {
		conn, err = net.DialTimeout("tcp", addr, 800*time.Millisecond)
		if err == nil {
			client := rpc.NewClient(conn)
			return client, err
		} else {
			if strings.Contains(err.Error(), "refused") {
				return nil, err
			}
			logrus.Errorf("<GetClient> dial [%s] err: %v\n", addr, err)
		}
		time.Sleep(1500 * time.Millisecond)
	}
	return nil, err
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
