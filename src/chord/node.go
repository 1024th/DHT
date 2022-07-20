package chord

import (
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	successorListLen = 5
)

var (
	maintainInterval = 100 * time.Millisecond
)

type NodeRecord struct {
	Addr string
	ID   *big.Int
}

type ChordNode struct {
	server     *rpc.Server
	listener   net.Listener
	online     bool
	onlineLock sync.RWMutex

	NodeRecord
	predecessor     NodeRecord
	predecessorLock sync.RWMutex
	successorList   [successorListLen]NodeRecord
	successorLock   sync.RWMutex
	finger          [hashLength]NodeRecord
	fingerLock      sync.RWMutex
	curFinger       int
	data            map[string]string
	dataLock        sync.RWMutex
	backup          map[string]string
	backupLock      sync.RWMutex
}

type Pair struct {
	key   string
	value string
}

func (node *ChordNode) isOnline() bool {
	node.onlineLock.RLock()
	result := node.online
	node.onlineLock.RUnlock()
	return result
}

func (node *ChordNode) setOnline() {
	node.onlineLock.Lock()
	node.online = true
	node.onlineLock.Unlock()
}
func (node *ChordNode) setOffline() {
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
}

func (node *ChordNode) Initialize(addr string) {
	node.online = false
	node.Addr = addr
	node.ID = Hash(addr)
	node.data = make(map[string]string)
	node.backup = make(map[string]string)

	node.server = rpc.NewServer()
	node.server.Register(node)
	node.listener, _ = net.Listen("tcp", node.Addr)
}

func (node *ChordNode) PrintSelf() {
	info := fmt.Sprintf("I'm [%s], online: %v, predecessor: [%s], successors:", node.Addr, node.isOnline(), node.predecessor.Addr)
	for i := 0; i < successorListLen; i++ {
		info += fmt.Sprintf("[%s] ", node.successorList[i].Addr)
	}
	logrus.Infoln(info)
}

func (node *ChordNode) Serve() {
	defer func() {
		node.listener.Close()
	}()
	for node.isOnline() {
		conn, err := node.listener.Accept()
		if !node.isOnline() {
			break
		}

		if err != nil {
			logrus.Errorf("node %s, listener accept error, stop serving.", node.Addr)
			return
		}
		go node.server.ServeConn(conn)
	}
}

// func (node *ChordNode) Hello(_ string, reply *string) error {
// 	*reply = fmt.Sprintf("\"Hello!\", said by node [%s].\n", node.addr)
// 	logrus.Infoln(*reply)
// 	return nil
// }

// "Run" is called after calling "NewNode".
func (node *ChordNode) Run() {
}

/* "Create" and "Join" are called after calling "Run".
 * For a dhtNode, either "Create" or "Join" will be called, but not both. */

// Create a new network.
func (node *ChordNode) Create() {
	node.successorList[0] = NodeRecord{node.Addr, node.ID}
	for i := 0; i < hashLength; i++ {
		node.finger[i] = NodeRecord{node.Addr, node.ID}
	}
	node.setOnline()
	node.maintain()
	go node.Serve()
}

func (node *ChordNode) GetSuccessorList(_ string, result *[successorListLen]NodeRecord) error {
	node.successorLock.RLock()
	logrus.Infof("<GetSuccessorList> [%s] sucList: %s\n", node.Addr, sucListToString(&node.successorList))
	*result = node.successorList
	// for i := 0; i < successorListLen; i++ {
	// 	result[i].Addr = node.successorList[i].Addr
	// 	if node.successorList[i].ID != nil {
	// 		result[i].ID = &big.Int{}
	// 		(*result[i].ID) = *node.successorList[i].ID
	// 	} else {
	// 		result[i].ID = nil
	// 	}
	// }
	// logrus.Infof("<GetSuccessorList> result: %s\n", sucListToString(result))
	node.successorLock.RUnlock()
	return nil
}

// Join an existing network. Return "true" if join succeeded and "false" if not.
func (node *ChordNode) Join(addr string) bool {
	if node.online {
		logrus.Errorf("<Join> [%s] already joined")
		return false
	}
	var suc NodeRecord
	err := RemoteCall(addr, "ChordNode.FindSuccessor", Hash(addr), &suc.Addr)
	if err != nil {
		logrus.Errorf("<Join> [%s] call [%s].FindSuccessor err: %s\n", node.Addr, addr, err)
	}
	suc.ID = Hash(suc.Addr)
	var tmpList [successorListLen]NodeRecord
	RemoteCall(suc.Addr, "ChordNode.GetSuccessorList", nil, &tmpList)
	node.successorLock.Lock()
	node.successorList[0] = suc
	for i := 1; i < successorListLen; i++ {
		node.successorList[i] = tmpList[i-1]
	}
	node.successorLock.Unlock()

	node.fingerLock.Lock()
	node.finger[0] = suc
	node.fingerLock.Unlock()

	for i := 1; i < hashLength; i++ {
		start := hashAdd(node.ID, 1<<i)
		var fingerAddr string
		err = RemoteCall(suc.Addr, "ChordNode.FindSuccessor", start, &fingerAddr)
		if err != nil {
			logrus.Errorf("<Join> [%s] call [%s].FindSuccessor err: %s\n", node.Addr, suc.Addr, err.Error())
		} else {
			node.fingerLock.Lock()
			node.finger[i].Addr = fingerAddr
			node.finger[i].ID = Hash(fingerAddr)
			node.fingerLock.Unlock()
		}
	}
	node.setOnline()
	node.maintain()
	go node.Serve()
	return true
}

func (node *ChordNode) fixFinger() {
	t := hashAdd(node.ID, 1<<node.curFinger)
	var res string
	err := node.FindSuccessor(t, &res)
	if err != nil {
		logrus.Errorf("<fixFinger> [%s] Find Successor of %v err: %v", node.Addr, t, err)
		return
	}
	node.fingerLock.RLock()
	needUpdate := node.finger[node.curFinger].Addr != res
	node.fingerLock.RUnlock()
	if needUpdate {
		node.fingerLock.Lock()
		node.finger[node.curFinger].Addr = res
		node.finger[node.curFinger].ID = Hash(res)
		node.fingerLock.Unlock()
	}
	node.curFinger = (node.curFinger + 1) % hashLength
}

func (node *ChordNode) getPredecessor() NodeRecord {
	node.predecessorLock.RLock()
	defer node.predecessorLock.RUnlock()
	return node.predecessor
}

func (node *ChordNode) setPredecessor(newPre string) {
	node.predecessorLock.Lock()
	node.predecessor.Addr = newPre
	node.predecessor.ID = Hash(newPre)
	node.predecessorLock.Unlock()
}

func (node *ChordNode) GetPredecessor(_ string, addr *string) error {
	*addr = node.getPredecessor().Addr
	return nil
}

func sucListToString(sucList *[successorListLen]NodeRecord) string {
	var res string
	for i := 0; i < successorListLen; i++ {
		res += fmt.Sprintf("[%s] ", (*sucList)[i].Addr)
	}
	return res
}

func (node *ChordNode) stabilize() {
	logrus.Infof("<stabilize> [%s] online: %v\n", node.Addr, node.isOnline())

	suc := node.getOnlineSuccessor()
	logrus.Infof("<stabilize> [%s] online successor: [%s]\n", node.Addr, suc.Addr)
	var newSuc NodeRecord
	RemoteCall(suc.Addr, "ChordNode.GetPredecessor", "", &newSuc.Addr)
	newSuc.ID = Hash(newSuc.Addr)
	logrus.Infof("<stabilize> newSucID %v node.ID %v suc.ID %v\n", newSuc, node.ID, suc.ID)
	if newSuc.Addr != "" && contains(newSuc.ID, node.ID, suc.ID) {
		suc = newSuc
	}
	var tmpList [successorListLen]NodeRecord
	err := RemoteCall(suc.Addr, "ChordNode.GetSuccessorList", "", &tmpList)
	if err != nil {
		logrus.Infof("<stabilize> [%s] GetSuccessorList of [%s] err: %v\n", node.Addr, suc.Addr, err)
	}
	logrus.Infof("<stabilize> [%s] suc [%s]'s sucList: %s\n", node.Addr, suc.Addr, sucListToString(&tmpList))
	node.successorLock.Lock()
	node.successorList[0] = suc
	for i := 1; i < successorListLen; i++ {
		node.successorList[i] = tmpList[i-1]
	}
	node.successorLock.Unlock()
	logrus.Infof("<stabilize> [%s] will notify [%s]\n", node.Addr, suc.Addr)
	RemoteCall(suc.Addr, "ChordNode.Notify", node.Addr, nil)
}

func (node *ChordNode) addToBackup(preData map[string]string) {
	node.backupLock.Lock()
	for k, v := range preData {
		node.backup[k] = v
	}
	node.backupLock.Unlock()
}

func (node *ChordNode) AddToBackup(preData map[string]string, _ *string) error {
	node.backupLock.Lock()
	for k, v := range preData {
		node.backup[k] = v
	}
	node.backupLock.Unlock()
	return nil
}

func (node *ChordNode) GetData(_ string, res *map[string]string) error {
	node.dataLock.RLock()
	*res = node.data
	node.dataLock.RUnlock()
	return nil
}

func (node *ChordNode) Notify(newPre string, _ *string) error {
	logrus.Infof("<Notify> [%s] newPre [%s]\n", node.Addr, newPre)
	pre := node.getPredecessor()
	if pre.Addr == "" || contains(Hash(newPre), pre.ID, node.ID) {
		logrus.Infof("<Notify> [%s] set predecessor to [%s]\n", node.Addr, newPre)
		newPreData := make(map[string]string)
		err := RemoteCall(newPre, "ChordNode.GetData", "", &newPreData)
		if err != nil {
			logrus.Errorf("<Notify> [%s] get data from [%d] err: %v\n", node.Addr, newPre, err)
			return err
		}
		node.addToBackup(newPreData) // TODO: remove from node.data?
		node.setPredecessor(newPre)
	}
	return nil
}

func (node *ChordNode) mergeBackupToData() {
	node.backupLock.RLock()
	node.dataLock.Lock()
	for k, v := range node.backup {
		node.data[k] = v
	}
	node.dataLock.Unlock()
	node.backupLock.RUnlock()

	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()
}

func (node *ChordNode) backupDataToSuccessor() {
	suc := node.getOnlineSuccessor()
	RemoteCall(suc.Addr, "ChordNode.AddToBackup", node.data, nil)
}

func (node *ChordNode) checkPredecessor() {
	pre := node.getPredecessor()
	if pre.Addr != "" && !node.Ping(pre.Addr) {
		node.mergeBackupToData()
		node.backupDataToSuccessor()
	}
}

func (node *ChordNode) maintain() {
	go func() {
		for node.online {
			node.fixFinger()
			time.Sleep(maintainInterval)
		}
	}()
	go func() {
		for node.online {
			node.stabilize()
			time.Sleep(maintainInterval)
		}
	}()
	go func() {
		for node.online {
			node.checkPredecessor()
			time.Sleep(maintainInterval)
		}
	}()
}

// Quit from the network it is currently in.
// "Quit" will not be called before "Create" or "Join".
// For a dhtNode, "Quit" may be called for many times.
// For a quited node, call "Quit" again should have no effect.
func (node *ChordNode) Quit() {
	node.setOffline()
}

// Chord offers a way of "normal" quitting.
// For "force quit", the node quit the network without informing other nodes.
// "ForceQuit" will be checked by TA manually.
func (node *ChordNode) ForceQuit() {}

// Check whether the node represented by the IP address is in the network.
func (node *ChordNode) Ping(addr string) bool {
	client, err := GetClient(addr)
	if err != nil {
		logrus.Errorf("<Ping> [%s] ping [%s] err: %v\n", node.Addr, addr, err)
		return false
	}
	if client != nil {
		client.Close()
	}
	return true
}

func (node *ChordNode) FindSuccessor(id *big.Int, result *string) error {
	logrus.Infof("<FindSuccessor> [%s] find %s\n", node.Addr, id.String())
	suc := node.getOnlineSuccessor()
	logrus.Infof("<FindSuccessor> [%s] online successor: [%s]%s\n", node.Addr, suc.Addr, suc.ID.String())

	if id.Cmp(suc.ID) == 0 || contains(id, node.ID, suc.ID) {
		*result = suc.Addr
		return nil
	}
	p := node.closestPrecedingFinger(id)
	return RemoteCall(p, "ChordNode.FindSuccessor", id, result)
}

func (node *ChordNode) getOnlineSuccessor() NodeRecord {
	// node.PrintSelf()
	for i := 0; i < successorListLen; i++ {
		if node.Ping(node.successorList[i].Addr) {
			logrus.Infof("<getOnlineSuccessor> find [%s]'s successor: [%s]\n", node.Addr, node.successorList[i].Addr)
			return node.successorList[i]
		}
	}
	logrus.Errorf("<getOnlineSuccessor> cannot find successor of [%s]\n", node.Addr)
	return NodeRecord{}
}

func (node *ChordNode) closestPrecedingFinger(id *big.Int) (addr string) {
	for i := hashLength - 1; i >= 0; i-- {
		if !node.Ping(node.finger[i].Addr) {
			continue
		}
		if contains(node.finger[i].ID, node.ID, id) {
			return node.finger[i].Addr
		}
	}

	return node.getOnlineSuccessor().Addr
}

func (node *ChordNode) PutValue(pair Pair, _ *string) error {
	node.dataLock.Lock()
	node.data[pair.key] = pair.value
	node.dataLock.Unlock()
	suc := node.getOnlineSuccessor()
	err := RemoteCall(suc.Addr, "ChordNode.PutInBackup", pair, nil)
	if err != nil {
		logrus.Errorf("<DeleteValue> delete in [%s]'s backup, err: %v\n", suc.Addr, err)
		return err
	}
	return nil
}

// Put a key-value pair into the network (if KEY is already in the network, cover it)
func (node *ChordNode) Put(key string, value string) bool {
	if !node.online {
		logrus.Errorf("<Put> [%s] offline\n", node.Addr)
		return false
	}
	var target string
	err := node.FindSuccessor(Hash(key), &target)
	if err != nil {
		logrus.Errorf("<Put> [%s] find successor of key %v err: %v\n", node.Addr, key, err)
		return false
	}
	err = RemoteCall(target, "ChordNode.PutValue", Pair{key, value}, nil)
	if err != nil {
		logrus.Errorf("<Put> insert to [%s] err: %v\n", target, err)
		return false
	}
	return true
}

func (node *ChordNode) GetValue(key string, value *string) error {
	node.dataLock.RLock()
	var ok bool
	*value, ok = node.data[key]
	node.dataLock.RUnlock()
	if ok {
		return nil
	} else {
		return fmt.Errorf("key %v not found in [%p]", key, node)
	}
}

// Get a key-value pair from the network.
// Return "true" and the value if success, "false" otherwise.
func (node *ChordNode) Get(key string) (bool, string) {
	if !node.online {
		logrus.Errorf("<Get> [%s] offline\n", node.Addr)
		return false, ""
	}
	var target string
	err := node.FindSuccessor(Hash(key), &target)
	if err != nil {
		logrus.Errorf("<Get> [%s] find successor of key %v err: %v\n", node.Addr, key, err)
		return false, ""
	}
	var value string
	err = RemoteCall(target, "ChordNode.GetValue", key, &value)
	if err != nil {
		logrus.Errorf("<Get> get from [%s] err: %v\n", target, err)
		return false, ""
	}
	return true, value
}

func (node *ChordNode) DeleteValue(key string, _ *string) error {
	node.dataLock.Lock()
	_, ok := node.data[key]
	delete(node.data, key)
	node.dataLock.Unlock()
	if !ok {
		return fmt.Errorf("key %s not exist in [%s]", key, node.Addr)
	}
	suc := node.getOnlineSuccessor()
	err := RemoteCall(suc.Addr, "ChordNode.DeleteInBackup", key, nil)
	if err != nil {
		logrus.Errorf("<DeleteValue> delete in [%s]'s backup, err: %v\n", suc.Addr, err)
		return err
	}
	return nil
}

func (node *ChordNode) DeleteInBackup(key string, _ *string) error {
	node.backupLock.Lock()
	_, ok := node.backup[key]
	delete(node.backup, key)
	node.backupLock.Unlock()
	if !ok {
		return fmt.Errorf("key %s not exist in [%s]'s backup", key, node.Addr)
	}
	return nil
}

// Remove the key-value pair represented by KEY from the network.
// Return "true" if remove successfully, "false" otherwise.
func (node *ChordNode) Delete(key string) bool {
	if !node.online {
		logrus.Errorf("<Delete> [%s] offline\n", node.Addr)
		return false
	}
	var target string
	err := node.FindSuccessor(Hash(key), &target)
	if err != nil {
		logrus.Errorf("<Delete> [%s] find successor of key %v err: %v\n", node.Addr, key, err)
		return false
	}
	var value string
	err = RemoteCall(target, "ChordNode.GetValue", key, &value)
	if err != nil {
		logrus.Errorf("<Delete> delete from [%s] err: %v\n", target, err)
		return false
	}
	return true
}
