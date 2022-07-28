package kademlia

import (
	"context"
	"myrpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const IDLength = 20               // length of ID in bytes
const IDBitLength = IDLength << 3 // length of ID in bits
const (
	K                 = 20
	alpha             = 3
	RefreshInterval   = 5 * time.Second
	ReplicateInterval = 10 * time.Minute
	ExpireInterval    = 10 * time.Minute
	RepublishInterval  = 10 * time.Minute
)

type IDType [IDLength]byte

type NodeRecord struct {
	Addr string
	ID   IDType
}

type Node struct {
	server myrpc.Server
	online atomic.Value
	NodeRecord
	buckets [IDBitLength]KBucketType
	mu      sync.RWMutex
	data    DataType
}

func NewNodeRecord(address string) NodeRecord {
	return NodeRecord{address, Hash(address)}
}

func (node *Node) isOnline() bool {
	return node.online.Load().(bool)
}

func (node *Node) setOnline(v bool) {
	node.online.Store(v)
}

func (node *Node) Init(addr string) {
	node.setOnline(false)
	node.Addr = addr
	node.ID = Hash(addr)
	node.server.Init(&RPCNode{node})
	node.clear()
}

func (node *Node) clear() {
	node.data.Init()
}

func (node *Node) Run() {
	// logrus.Tracef("<Run> [%s]\n", node.name())
	node.setOnline(true)
	err := node.server.StartServing("tcp", node.Addr)
	if err != nil {
		logrus.Errorf("<Run> [%s] start serving err: %v\n", node.name(), err)
	}
	node.maintain()
}

func (node *Node) Update(contact NodeRecord) {
	if contact.Addr == "" || contact.Addr == node.Addr {
		return
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	node.buckets[PrefixLen(Xor(node.ID, contact.ID))].Update(contact)
}

func (node *Node) CheckOnline(contact NodeRecord) bool {
	err := myrpc.RemoteCall(contact.Addr, "RPCNode.Ping", node.Addr, nil)
	if err != nil {
		logrus.Errorf("<CheckOnline> [%s] ping [%s] err: %v\n", node.name(), contact.name(), err)
		return false
	}
	return true
}

func (node *Node) Ping(address string) bool {
	return node.CheckOnline(NewNodeRecord(address))
}

func (node *Node) Join(address string) bool {
	logrus.Infof("<Join> [%s] join [%s]\n", node.name(), getPortFromIP(address))
	contact := NewNodeRecord(address)
	if !node.CheckOnline(contact) {
		logrus.Errorf("<Join> [%s] join [%s] fail\n", node.name(), address)
		return false
	}
	node.Update(contact)
	node.FindClosestNode(node.ID)
	return true
}

func (node *Node) FindNode(target IDType) OrderedNodeList {
	node.mu.RLock()
	defer node.mu.RUnlock()
	var res OrderedNodeList
	res.Target = target
	for i := 0; i < IDBitLength; i++ {
		for j := 0; j < node.buckets[i].Size; j++ {
			res.Insert(node.buckets[i].List[j])
		}
	}
	return res
}

func (node *Node) FindValue(key string) FindValueReply {
	node.mu.RLock()
	defer node.mu.RUnlock()
	val, ok := node.data.Load(key)
	if ok {
		return FindValueReply{OrderedNodeList{}, val}
	}
	var res OrderedNodeList
	res.Target = Hash(key)
	for i := 0; i < IDBitLength; i++ {
		for j := 0; j < node.buckets[i].Size; j++ {
			res.Insert(node.buckets[i].List[j])
		}
	}
	return FindValueReply{res, ""}
}

func (node *Node) FindClosestNode(target IDType) OrderedNodeList {
	l := node.FindNode(target)
	l.Insert(node.NodeRecord)

	updated := true
	vis := make(map[string]bool)

	for updated {
		updated = false
		var tmpList OrderedNodeList
		var removeList []NodeRecord
		for i := 0; i < l.Size; i++ {
			if vis[l.List[i].Addr] {
				continue
			}
			vis[l.List[i].Addr] = true
			node.Update(l.List[i])
			var reply OrderedNodeList
			err := myrpc.RemoteCall(l.List[i].Addr, "RPCNode.FindNode", FindNodeArg{target, node.Addr}, &reply)
			if err != nil {
				logrus.Errorf("<FindClosestNode> [%s] call [%s].FindNode err: %v\n", node.name(), l.List[i].name(), err)
				removeList = append(removeList, l.List[i])
			} else {
				for j := 0; j < reply.Size; j++ {
					tmpList.Insert(reply.List[j])
				}
			}
		}
		for _, v := range removeList {
			l.Remove(v)
		}
		for i := 0; i < tmpList.Size; i++ {
			updated = updated || l.Insert(tmpList.List[i])
		}
	}

	return l
}

func (node *Node) Get(key string) (ok bool, value string) {
	vis := make(map[string]bool)
	reply := node.FindValue(key)
	if reply.Value != "" {
		return true, reply.Value
	}
	l := reply.List

	updated := true
	for updated {
		updated = false
		var tmpList OrderedNodeList
		var removeList []NodeRecord
		for i := 0; i < l.Size; i++ {
			if vis[l.List[i].Addr] {
				continue
			}
			vis[l.List[i].Addr] = true
			var reply FindValueReply
			err := myrpc.RemoteCall(l.List[i].Addr, "RPCNode.FindValue", FindValueArg{key, node.Addr}, &reply)
			if err != nil {
				logrus.Errorf("<FindClosestNode> [%s] call [%s].FindNode err: %v\n", node.name(), l.List[i].name(), err)
				removeList = append(removeList, l.List[i])
			} else {
				if reply.Value != "" {
					return true, reply.Value
				}
				for j := 0; j < reply.List.Size; j++ {
					tmpList.Insert(reply.List.List[j])
				}
			}
		}
		for _, key1 := range removeList {
			l.Remove(key1)
		}
		for i := 0; i < tmpList.Size; i++ {
			updated = updated || l.Insert(tmpList.List[i])
		}
	}

	return false, ""
}

func (node *Node) iterativeStore(key string, value string) bool {
	keyID := Hash(key)

	l := node.FindClosestNode(keyID)
	l.Insert(NewNodeRecord(node.Addr))
	for i := 0; i < l.Size; i++ {
		err := myrpc.RemoteCall(l.List[i].Addr, "RPCNode.Store", StoreArg{key, value, node.Addr}, nil)
		if err != nil {
			logrus.Errorf("<Put> [%s] call [%s].Store err: %v\n", node.name(), l.List[i].name(), err)
		}
	}

	return true
}

func (node *Node) Put(key string, value string) bool {
	return node.iterativeStore(key, value)
}

func (node *Node) Refresh() {
	// TODO
}

func (node *Node) Replicate() {
	// TODO
}

func (node *Node) Republish() {
	data := node.data.GetData()
	republishList := node.data.RepublishList()
	for _, key := range republishList {
		node.Put(key, data[key])
	}
}

func (node *Node) maintain() {
	go func() {
		for node.isOnline() {
			node.Refresh()
			time.Sleep(RefreshInterval)
		}
	}()
	go func() {
		for node.isOnline() {
			node.Replicate()
			time.Sleep(ReplicateInterval)
		}
	}()
	go func() {
		for node.isOnline() {
			node.data.Expire()
			time.Sleep(ExpireInterval)
		}
	}()
	go func() {
		for node.isOnline() {
			node.Republish()
			time.Sleep(RepublishInterval)
		}
	}()
}

func (node *Node) Delete(key string) bool {
	node.data.Delete(key)
	return true
}

func (node *Node) Create() {
}
func (node *Node) Quit() {
	node.clear()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	node.server.Shutdown(ctx)
}

func (node *Node) ForceQuit() {
	node.clear()
	node.server.Close()
}

func (node NodeRecord) name() string {
	return getPortFromIP(node.Addr)
}
func getPortFromIP(ip string) string {
	return ip[strings.Index(ip, ":")+1:]
}
