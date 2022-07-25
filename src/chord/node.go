package chord

import (
	"context"
	"fmt"
	"math/big"
	"myrpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	successorListLen = 10
)

const (
	checkPredecessorInterval = 200 * time.Millisecond
	stabilizeInterval        = 200 * time.Millisecond
	fixFingerInterval        = 200 * time.Millisecond
)

type NodeRecord struct {
	Addr string
	ID   *big.Int
}

type Node struct {
	server myrpc.Server
	online atomic.Value

	NodeRecord
	predecessor     NodeRecord
	predecessorLock sync.RWMutex
	successorList   [successorListLen]NodeRecord
	successorLock   sync.RWMutex
	finger          [hashLength]NodeRecord
	fingerLock      sync.RWMutex
	curFinger       uint
	data            map[string]string
	dataLock        sync.RWMutex
	backup          map[string]string
	backupLock      sync.RWMutex
}

type Pair struct {
	Key   string
	Value string
}

func (node *Node) isOnline() bool {
	return node.online.Load().(bool)
}

func (node *Node) setOnline(v bool) {
	node.online.Store(v)
}

func (node *Node) Initialize(addr string) {
	node.online.Store(false)
	node.Addr = addr
	node.ID = Hash(addr)
	node.clear()
	node.server.Init(&RPCNode{node})
}

func (node *Node) PrintSelf() {
	info := fmt.Sprintf("I'm [%s], online: %v, predecessor: [%s], successors:", node.name(), node.isOnline(), node.predecessor.name())
	for i := 0; i < successorListLen; i++ {
		info += fmt.Sprintf("[%s] ", node.successorList[i].name())
	}
	info += "\n"
	logrus.Info(info)
}

func (node *Node) Hello(reply *string) error {
	*reply = fmt.Sprintf("\"Hello!\", said by node [%s].\n", node.name())
	logrus.Infoln(*reply)
	return nil
}

// "Run" is called after calling "NewNode".
func (node *Node) Run() {
	// logrus.Tracef("<Run> [%s]\n", node.name())
	node.setOnline(true)
	err := node.server.StartServing("tcp", node.Addr)
	if err != nil {
		logrus.Errorf("<Run> [%s] start serving err: %v\n", node.name(), err)
	}
}

/* "Create" and "Join" are called after calling "Run".
 * For a dhtNode, either "Create" or "Join" will be called, but not both. */

// Create a new network.
func (node *Node) Create() {
	node.successorList[0] = NodeRecord{node.Addr, node.ID}
	for i := 0; i < hashLength; i++ {
		node.finger[i] = NodeRecord{node.Addr, node.ID}
	}
	node.setOnline(true)
	node.maintain()
}

func (node *Node) GetSuccessorList(result *[successorListLen]NodeRecord) error {
	node.successorLock.RLock()
	logrus.Infof("<GetSuccessorList> [%s] sucList: %s\n", node.name(), sucListToString(&node.successorList))
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

func (node *Node) ShrinkBackup(removeFromBak map[string]string) error {
	node.backupLock.Lock()
	for k := range removeFromBak {
		delete(node.backup, k)
	}
	node.backupLock.Unlock()
	return nil
}

func (node *Node) TransferData(preAddr string, preData *map[string]string) error {
	preID := Hash(preAddr)
	node.dataLock.Lock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	for k, v := range node.data {
		kID := Hash(k)
		if kID.Cmp(node.ID) != 0 && !contains(kID, preID, node.ID) {
			(*preData)[k] = v
			node.backup[k] = v
			delete(node.data, k)
		}
	}
	node.dataLock.Unlock()
	node.backupLock.Unlock()
	suc := node.getOnlineSuccessor()
	err := myrpc.RemoteCall(suc.Addr, "RPCNode.ShrinkBackup", *preData, nil)
	if err != nil {
		logrus.Errorf("<TransferData> [%s] pre [%s] err: %v\n", node.name(), preAddr, err)
		return err
	}
	// node.Notify(preAddr, nil)
	node.setPredecessor(preAddr)
	return nil
}

// Join an existing network. Return "true" if join succeeded and "false" if not.
func (node *Node) Join(addr string) bool {
	// if node.isOnline() {
	// 	logrus.Errorf("<Join> [%s] already joined")
	// 	return false
	// }
	logrus.Infof("<Join> [%s] join [%s]\n", node.name(), getPortFromIP(addr))
	var suc NodeRecord
	err := myrpc.RemoteCall(addr, "RPCNode.FindSuccessor", FindSucInput{node.ID, 0}, &suc.Addr)
	if err != nil {
		logrus.Errorf("<Join> [%s] call [%s].FindSuccessor err: %s\n", node.name(), addr, err)
		return false
	}
	suc.ID = Hash(suc.Addr)
	var tmpList [successorListLen]NodeRecord
	err = myrpc.RemoteCall(suc.Addr, "RPCNode.GetSuccessorList", Void{}, &tmpList)
	if err != nil {
		logrus.Errorf("<Join> [%s] call [%s].GetSuccessorList err: %s\n", node.name(), addr, err)
	}
	node.successorLock.Lock()
	node.successorList[0] = suc
	for i := 1; i < successorListLen; i++ {
		node.successorList[i] = tmpList[i-1]
	}
	node.successorLock.Unlock()

	node.fingerLock.Lock()
	node.finger[0] = suc
	node.fingerLock.Unlock()

	// for i := 1; i < hashLength; i++ {
	// 	start := hashAdd(node.ID, 1<<i)
	// 	var fingerAddr string
	// 	err = RemoteCall(suc.Addr, "RPCNode.FindSuccessor", start, &fingerAddr)
	// 	if err != nil {
	// 		logrus.Errorf("<Join> [%s] call [%s].FindSuccessor err: %s\n", node.name(), suc.name(), err.Error())
	// 	} else {
	// 		node.fingerLock.Lock()
	// 		node.finger[i].Addr = fingerAddr
	// 		node.finger[i].ID = Hash(fingerAddr)
	// 		node.fingerLock.Unlock()
	// 	}
	// }
	node.dataLock.Lock()
	err = myrpc.RemoteCall(suc.Addr, "RPCNode.TransferData", node.Addr, &node.data)
	node.dataLock.Unlock()
	if err != nil {
		logrus.Errorf("<Join> [%s] TransferData from suc [%s] err: %v\n", node.name(), suc.name(), err)
		return false
	}

	node.setOnline(true)
	node.maintain()
	// node.Stabilize()
	// time.Sleep(200 * time.Millisecond)
	return true
}

func (node *Node) fixFinger() {
	t := hashCalc(node.ID, node.curFinger)
	var res string
	err := node.FindSuccessor(FindSucInput{t, 0}, &res)
	if err != nil {
		logrus.Errorf("<fixFinger> [%s] Find Successor of %v err: %v\n", node.name(), t, err)
		return
	}
	node.fingerLock.Lock()
	if node.finger[node.curFinger].Addr != res {
		node.finger[node.curFinger].Addr = res
		node.finger[node.curFinger].ID = Hash(res)
	}
	node.fingerLock.Unlock()
	node.curFinger = (node.curFinger + 1) % hashLength
}

func (node *Node) getPredecessor() NodeRecord {
	node.predecessorLock.RLock()
	defer node.predecessorLock.RUnlock()
	return node.predecessor
}

func (node *Node) setPredecessor(newPre string) {
	node.predecessorLock.Lock()
	node.predecessor.Addr = newPre
	node.predecessor.ID = Hash(newPre)
	node.predecessorLock.Unlock()
}

func (node *Node) GetPredecessor(addr *string) error {
	*addr = node.getPredecessor().Addr
	return nil
}

func sucListToString(sucList *[successorListLen]NodeRecord) string {
	var res string
	for i := 0; i < successorListLen; i++ {
		res += fmt.Sprintf("[%s] ", (*sucList)[i].name())
	}
	return res
}

func (node *Node) Stabilize() error {
	logrus.Infof("<Stabilize> [%s] online: %v\n", node.name(), node.isOnline())

	suc := node.getOnlineSuccessor()
	logrus.Infof("<Stabilize> [%s] online successor: [%s]\n", node.name(), suc.name())
	var newSuc NodeRecord
	err := myrpc.RemoteCall(suc.Addr, "RPCNode.GetPredecessor", Void{}, &newSuc.Addr)
	if err != nil {
		logrus.Errorf("<Stabilize> [%s] GetPredecessor of [%s] err: %v\n", node.name(), suc.name(), err)
		return fmt.Errorf("<Stabilize> [%s] GetPredecessor of [%s] err: %v", node.name(), suc.name(), err)
	}
	newSuc.ID = Hash(newSuc.Addr)
	logrus.Infof("<Stabilize> newSucID %v node.ID %v suc.ID %v\n", newSuc, node.ID, suc.ID)
	if newSuc.Addr != "" && contains(newSuc.ID, node.ID, suc.ID) {
		suc = newSuc
	}
	var tmpList [successorListLen]NodeRecord
	err = myrpc.RemoteCall(suc.Addr, "RPCNode.GetSuccessorList", Void{}, &tmpList)
	if err != nil {
		logrus.Errorf("<Stabilize> [%s] GetSuccessorList of [%s] err: %v\n", node.name(), suc.name(), err)
		return fmt.Errorf("<Stabilize> [%s] GetSuccessorList of [%s] err: %v", node.name(), suc.name(), err)
	}
	logrus.Infof("<Stabilize> [%s] suc [%s]'s sucList: %s\n", node.name(), suc.name(), sucListToString(&tmpList))
	node.fingerLock.Lock()
	node.finger[0] = suc
	node.fingerLock.Unlock()
	node.successorLock.Lock()
	node.successorList[0] = suc
	for i := 1; i < successorListLen; i++ {
		node.successorList[i] = tmpList[i-1]
	}
	node.successorLock.Unlock()
	logrus.Infof("<Stabilize> [%s] will notify [%s]\n", node.name(), suc.name())
	err = myrpc.RemoteCall(suc.Addr, "RPCNode.Notify", node.Addr, nil)
	if err != nil {
		logrus.Errorf("<Stabilize> [%s] notify [%s] err: %v\n", node.name(), suc.name(), err)
	}
	return nil
}

func (node *Node) addToBackup(preData map[string]string) {
	node.backupLock.Lock()
	for k, v := range preData {
		node.backup[k] = v
	}
	node.backupLock.Unlock()
}

func (node *Node) AddToBackup(preData map[string]string) error {
	node.backupLock.Lock()
	for k, v := range preData {
		node.backup[k] = v
	}
	node.backupLock.Unlock()
	return nil
}

func (node *Node) GetData(res *map[string]string) error {
	node.dataLock.RLock()
	*res = node.data
	node.dataLock.RUnlock()
	return nil
}

func (node *Node) Notify(newPre string) error {
	logrus.Infof("<Notify> [%s] newPre [%s]\n", node.name(), getPortFromIP(newPre))
	pre := node.getPredecessor()
	if node.Ping(newPre) && (pre.Addr == "" || contains(Hash(newPre), pre.ID, node.ID)) {
		logrus.Infof("<Notify> [%s] set predecessor to [%s]\n", node.name(), newPre)
		node.setPredecessor(newPre)
		newPreData := make(map[string]string)
		err := myrpc.RemoteCall(newPre, "RPCNode.GetData", Void{}, &newPreData)
		if err != nil {
			logrus.Errorf("<Notify> [%s] get data from [%s] err: %v\n", node.name(), newPre, err)
			return err
		}
		node.backup = newPreData
		// node.addToBackup(newPreData) // TODO: remove from node.data?
	}
	return nil
}

func (node *Node) mergeBackupToData() {
	node.backupLock.RLock()
	tmpBackup := node.backup
	node.backupLock.RUnlock()
	node.dataLock.Lock()
	for k, v := range tmpBackup {
		node.data[k] = v
	}
	node.dataLock.Unlock()

	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()
}

func (node *Node) backupDataToSuccessor() {
	suc := node.getOnlineSuccessor()
	myrpc.RemoteCall(suc.Addr, "RPCNode.AddToBackup", node.data, nil)
}

func (node *Node) CheckPredecessor() error {
	pre := node.getPredecessor()
	// logrus.Infof("<CheckPredecessor> [%s] pre [%s] ping %v\n", node.name(), pre.name(), node.Ping(pre.Addr))
	if pre.Addr != "" && !node.Ping(pre.Addr) {
		logrus.Warnf("<CheckPredecessor> [%s] fail\n", node.Addr)
		node.setPredecessor("")
		node.mergeBackupToData()
		node.backupDataToSuccessor()
	}
	return nil
}

func (node *Node) maintain() {
	go func() {
		for node.isOnline() {
			node.fixFinger()
			time.Sleep(fixFingerInterval)
		}
	}()
	go func() {
		for node.isOnline() {
			node.Stabilize()
			time.Sleep(stabilizeInterval)
		}
	}()
	go func() {
		for node.isOnline() {
			node.CheckPredecessor()
			time.Sleep(checkPredecessorInterval)
		}
	}()
}

func (node *Node) clear() {
	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()
}

// Quit from the network it is currently in.
// "Quit" will not be called before "Create" or "Join".
// For a dhtNode, "Quit" may be called for many times.
// For a quited node, call "Quit" again should have no effect.
func (node *Node) Quit() {
	if !node.isOnline() {
		logrus.Warnf("<Quit> [%s] offline\n", node.name())
		return
	}
	logrus.Infof("<Quit> [%s]\n", node.Addr)
	node.setOnline(false)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	node.server.Shutdown(ctx)
	suc := node.getOnlineSuccessor()
	err := myrpc.RemoteCall(suc.Addr, "RPCNode.CheckPredecessor", Void{}, nil)
	if err != nil {
		logrus.Errorf("<Quit> [%s] call [%s] CheckPredecessor err: %v\n", node.name(), suc.name(), err)
	}
	pre := node.getPredecessor()
	err = myrpc.RemoteCall(pre.Addr, "RPCNode.Stabilize", Void{}, nil)
	if err != nil {
		logrus.Errorf("<Quit> [%s] call [%s] Stabilize err: %v\n", node.name(), pre.name(), err)
	}
	node.clear()
	// time.Sleep(300 * time.Millisecond)
}

// Chord offers a way of "normal" quitting.
// For "force quit", the node quit the network without informing other nodes.
// "ForceQuit" will be checked by TA manually.
func (node *Node) ForceQuit() {
	if !node.isOnline() {
		logrus.Warnf("<ForceQuit> [%s] offline\n", node.name())
		return
	}
	logrus.Infof("<ForceQuit> [%s]\n", node.Addr)
	node.setOnline(false)
	node.server.Close()
	node.clear()
}

// Check whether the node represented by the IP address is in the network.
func (node *Node) Ping(addr string) bool {
	return myrpc.Ping(addr)
}

type FindSucInput struct {
	ID    *big.Int
	Depth int
}

func (node *Node) FindSuccessor(input FindSucInput, result *string) error {
	id := input.ID
	if !node.isOnline() {
		return fmt.Errorf("<FindSuccessor> [%s] offline", node.name())
	}
	// logrus.Infof("<FindSuccessor> [%s] find %s\n", node.name(), id.String())
	suc := node.getOnlineSuccessor()
	if suc.Addr == "" {
		return fmt.Errorf("cannot find successor of [%s]", node.Addr)
	}
	logrus.Infof("<FindSuccessor> [%s] depth: %d, online successor: [%s]\n", node.name(), input.Depth, suc.name())
	// logrus.Infof("<FindSuccessor> [%s] target: %s self: %s suc: %s contains:%v\n", node.name(), id, node.ID, suc.ID.String(), contains(id, node.ID, suc.ID))

	if id.Cmp(suc.ID) == 0 || contains(id, node.ID, suc.ID) {
		*result = suc.Addr
		logrus.Infof("<FindSuccessor> [%s] depth: %d found successor [%s] is target, id: %s self: %s suc: %s\n", node.name(), input.Depth, suc.name(), id, node.ID, suc.ID)
		return nil
	}
	p := node.closestPrecedingFinger(id)
	return myrpc.RemoteCall(p, "RPCNode.FindSuccessor", FindSucInput{id, input.Depth + 1}, result)
}

func (node *Node) getOnlineSuccessor() NodeRecord {
	// node.PrintSelf()
	for i := 0; i < successorListLen; i++ {
		node.successorLock.RLock()
		suc := node.successorList[i]
		node.successorLock.RUnlock()
		if suc.Addr != "" && node.Ping(suc.Addr) {
			// logrus.Infof("<getOnlineSuccessor> find [%s]'s successor: [%s]\n", node.name(), node.successorList[i].Addr)
			// if i > 0 {
			// 	node.successorLock.Lock()
			// 	for j := i; j < successorListLen; j++ {
			// 		node.successorList[j-i] = node.successorList[j]
			// 	}
			// 	node.successorLock.Unlock()
			// 	RemoteCall(suc.Addr, "RPCNode.Notify", node.Addr, nil)
			// }
			return suc
		}
	}
	logrus.Errorf("<getOnlineSuccessor> cannot find successor of [%s]\n", node.Addr)
	return NodeRecord{}
}

func (node *Node) closestPrecedingFinger(id *big.Int) (addr string) {
	for i := hashLength - 1; i >= 0; i-- {
		node.fingerLock.RLock()
		fin := node.finger[i]
		node.fingerLock.RUnlock()
		if fin.Addr == "" {
			continue
		}
		if !node.Ping(fin.Addr) {
			node.fingerLock.Lock()
			node.finger[i].Addr = ""
			node.finger[i].ID = nil
			node.fingerLock.Unlock()
			continue
		}
		if contains(fin.ID, node.ID, id) {
			return fin.Addr
		}
	}

	return node.getOnlineSuccessor().Addr
}

func (node *Node) PutValue(pair Pair) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	suc := node.getOnlineSuccessor()
	err := myrpc.RemoteCall(suc.Addr, "RPCNode.PutInBackup", pair, nil)
	if err != nil {
		logrus.Errorf("<PutValue> [%s] put in [%s]'s backup, err: %v\n", node.name(), suc.name(), err)
		return err
	}
	return nil
}

func (node *Node) PutInBackup(pair Pair) error {
	node.backupLock.Lock()
	node.backup[pair.Key] = pair.Value
	node.backupLock.Unlock()
	return nil
}

// Put a key-value pair into the network (if KEY is already in the network, cover it)
func (node *Node) Put(key string, value string) bool {
	if !node.isOnline() {
		logrus.Errorf("<Put> [%s] offline\n", node.Addr)
		return false
	}
	var target string
	err := node.FindSuccessor(FindSucInput{Hash(key), 0}, &target)
	if err != nil {
		logrus.Errorf("<Put> [%s] find successor of key %v err: %v\n", node.name(), key, err)
		return false
	}
	err = myrpc.RemoteCall(target, "RPCNode.PutValue", Pair{key, value}, nil)
	if err != nil {
		logrus.Errorf("<Put> insert to [%s] err: %v\n", target, err)
		return false
	}
	return true
}

func (node *Node) GetValue(key string, value *string) error {
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
func (node *Node) Get(key string) (bool, string) {
	if !node.isOnline() {
		logrus.Errorf("<Get> [%s] offline\n", node.Addr)
		return false, ""
	}
	var target string
	err := node.FindSuccessor(FindSucInput{Hash(key), 0}, &target)
	if err != nil {
		logrus.Errorf("<Get> [%s] find successor of key %v err: %v\n", node.name(), key, err)
		return false, ""
	}
	var value string
	err = myrpc.RemoteCall(target, "RPCNode.GetValue", key, &value)
	if err != nil {
		logrus.Errorf("<Get> get from [%s] err: %v\n", target, err)
		return false, ""
	}
	return true, value
}

func (node *Node) DeleteValue(key string) error {
	node.dataLock.Lock()
	_, ok := node.data[key]
	delete(node.data, key)
	node.dataLock.Unlock()
	if !ok {
		return fmt.Errorf("key %s not exist in [%s]", key, node.Addr)
	}
	suc := node.getOnlineSuccessor()
	err := myrpc.RemoteCall(suc.Addr, "RPCNode.DeleteInBackup", key, nil)
	if err != nil {
		logrus.Errorf("<DeleteValue> delete in [%s]'s backup, err: %v\n", suc.name(), err)
		return err
	}
	return nil
}

func (node *Node) DeleteInBackup(key string) error {
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
func (node *Node) Delete(key string) bool {
	if !node.isOnline() {
		logrus.Errorf("<Delete> [%s] offline\n", node.Addr)
		return false
	}
	var target string
	err := node.FindSuccessor(FindSucInput{Hash(key), 0}, &target)
	if err != nil {
		logrus.Errorf("<Delete> [%s] find successor of key %v err: %v\n", node.name(), key, err)
		return false
	}
	err = myrpc.RemoteCall(target, "RPCNode.DeleteValue", key, nil)
	if err != nil {
		logrus.Errorf("<Delete> delete from [%s] err: %v\n", target, err)
		return false
	}
	return true
}

func (node NodeRecord) name() string {
	return getPortFromIP(node.Addr)
}

func getPortFromIP(ip string) string {
	return ip[strings.Index(ip, ":")+1:]
}
