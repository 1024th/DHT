package chord

import (
	"errors"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var clientPools sync.Map // map (address string) -> *ClientPool
var (
	CheckConnInterval     = 10 * time.Second
	DialTimeout           = 800 * time.Millisecond
	SleepAfterDialTimeout = 1500 * time.Millisecond
	DialRetryNum          = 3
)

type ClientRes struct {
	client *rpc.Client
	time   time.Time // when the client was created
}

type ClientPool struct {
	clients chan ClientRes
}

func NewClientPool(maxCap int) (*ClientPool, error) {
	return &ClientPool{make(chan ClientRes, maxCap)}, nil
}

func CreateClient(address string) (ClientRes, error) {
	logrus.Infof("<CreateClient> addr [%s]\n", address)
	var conn net.Conn
	var err error
	for i := 0; i < DialRetryNum; i++ {
		conn, err = net.DialTimeout("tcp", address, DialTimeout)
		if err == nil {
			client := rpc.NewClient(conn)
			return ClientRes{client, time.Now()}, err
		} else {
			if strings.Contains(err.Error(), "refused") {
				return ClientRes{}, err
			}
			logrus.Errorf("<CreateClient> dial [%s] err: %v\n", address, err)
		}
		time.Sleep(SleepAfterDialTimeout)
	}
	return ClientRes{}, err
}

func (pool *ClientPool) Get(address string) (ClientRes, error) {
	for {
		select {
		case c := <-pool.clients:
			if time.Since(c.time) > CheckConnInterval {
				err := c.client.Call("RPCHeartbeatRcvr.Heartbeat", struct{}{}, nil)
				if err != nil {
					logrus.Warnf("<pool.Get> %p heartbeat fail, remove it\n", c.client)
					c.client.Close()
					continue
				} else {
					c.time = time.Now()
				}
			}
			// logrus.Infof("<pool.Get> get from pool %p\n", c.client)
			return c, nil
		default:
			return CreateClient(address)
		}
	}
}

func (pool *ClientPool) Put(c ClientRes) error {
	if c.client == nil {
		return errors.New("client is nil. rejecting")
	}

	if pool.clients == nil {
		// logrus.Infof("<pool.Put> close client %p\n", c)
		return c.client.Close()
	}

	select {
	case pool.clients <- c:
		// logrus.Infof("<pool.Put> put into pool %p, pool len: %v\n", c.client, len(pool.clients))
		return nil
	default:
		// logrus.Infof("<pool.Put> close client %p\n", c.client)
		return c.client.Close()
	}
}

func GetClient(address string) (ClientRes, error) {
	pool, ok := clientPools.Load(address)
	// logrus.Infof("<GetClient> clientPools len: %v\n", len(clientPools))
	if ok {
		return pool.(*ClientPool).Get(address)
	} else {
		var err error
		logrus.Infof("<GetClient> NewClientPool [%s]\n", address)
		pool, err = NewClientPool(5)
		if err != nil {
			return ClientRes{}, err
		}
		clientPools.Store(address, pool)
		return pool.(*ClientPool).Get(address)
	}
}

func PutClient(address string, client ClientRes) error {
	pool, ok := clientPools.Load(address)
	if ok {
		return pool.(*ClientPool).Put(client)
	} else {
		var err error
		pool, err = NewClientPool(5)
		if err != nil {
			return err
		}
		clientPools.Store(address, pool)
		return pool.(*ClientPool).Put(client)
	}
}

func Ping(addr string) bool {
	c, err := GetClient(addr)
	if err != nil {
		logrus.Warnf("<Ping> GetClient [%s] err: %v\n", addr, err)
		return false
	}
	err = c.client.Call("RPCHeartbeatRcvr.Heartbeat", struct{}{}, nil)
	if err != nil {
		logrus.Warnf("<Ping> %p heartbeat fail\n", c.client)
		c.client.Close()
		return false
	} else {
		PutClient(addr, c)
		return true
	}
}

func RemoteCall(addr string, serviceMethod string, args interface{}, reply interface{}) error {
	c, err := GetClient(addr)
	if err != nil {
		return err
	}
	// if client != nil {
	// 	defer client.Close()
	// }
	err = c.client.Call(serviceMethod, args, reply)
	if err == nil {
		PutClient(addr, c)
	} else {
		c.client.Close()
	}
	return err
}
