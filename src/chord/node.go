package chord

import (
	"context"
	"fmt"
	"math/big"
	"net"
	"runtime"
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
	pingTimeout              = 500 * time.Millisecond
)

type NodeRecord struct {
	Addr string
	ID   *big.Int
}

type ChordNode struct {
	server RPCServer
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

func (node *ChordNode) isOnline() bool {
	return node.online.Load().(bool)
}

func (node *ChordNode) setOnline(v bool) {
	node.online.Store(v)
}

func (node *ChordNode) Initialize(addr string) {
	node.online.Store(false)
	node.Addr = addr
	node.ID = Hash(addr)
	node.clear()
	node.server.Init(&RPCNode{node})
}

func (node *ChordNode) PrintSelf() {
	info := fmt.Sprintf("I'm [%s], online: %v, predecessor: [%s], successors:", node.name(), node.isOnline(), node.predecessor.name())
	for i := 0; i < successorListLen; i++ {
		info += fmt.Sprintf("[%s] ", node.successorList[i].name())
	}
	info += "\n"
	logrus.Info(info)
}

func (node *ChordNode) Hello(_ string, reply *string) error {
	*reply = fmt.Sprintf("\"Hello!\", said by node [%s].\n", node.name())
	logrus.Infoln(*reply)
	return nil
}

// "Run" is called after calling "NewNode".
func (node *ChordNode) Run() {
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
func (node *ChordNode) Create() {
	node.successorList[0] = NodeRecord{node.Addr, node.ID}
	for i := 0; i < hashLength; i++ {
		node.finger[i] = NodeRecord{node.Addr, node.ID}
	}
	node.setOnline(true)
	node.maintain()
}

func (node *ChordNode) GetSuccessorList(_ string, result *[successorListLen]NodeRecord) error {
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

func (node *ChordNode) ShrinkBackup(removeFromBak map[string]string, _ *string) error {
	node.backupLock.Lock()
	for k := range removeFromBak {
		delete(node.backup, k)
	}
	node.backupLock.Unlock()
	return nil
}

func (node *ChordNode) TransferData(preAddr string, preData *map[string]string) error {
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
	err := RemoteCall(suc.Addr, "RPCNode.ShrinkBackup", *preData, nil)
	if err != nil {
		logrus.Errorf("<TransferData> [%s] pre [%s] err: %v\n", node.name(), preAddr, err)
		return err
	}
	// node.Notify(preAddr, nil)
	node.setPredecessor(preAddr)
	return nil
}

// Join an existing network. Return "true" if join succeeded and "false" if not.
func (node *ChordNode) Join(addr string) bool {
	// if node.isOnline() {
	// 	logrus.Errorf("<Join> [%s] already joined")
	// 	return false
	// }
	logrus.Infof("<Join> [%s] join [%s]\n", node.name(), getPortFromIP(addr))
	var suc NodeRecord
	err := RemoteCall(addr, "RPCNode.FindSuccessor", FindSucInput{node.ID, 0}, &suc.Addr)
	if err != nil {
		logrus.Errorf("<Join> [%s] call [%s].FindSuccessor err: %s\n", node.name(), addr, err)
		return false
	}
	suc.ID = Hash(suc.Addr)
	var tmpList [successorListLen]NodeRecord
	RemoteCall(suc.Addr, "RPCNode.GetSuccessorList", nil, &tmpList)
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
	err = RemoteCall(suc.Addr, "RPCNode.TransferData", node.Addr, &node.data)
	node.dataLock.Unlock()
	if err != nil {
		logrus.Errorf("<Join> [%s] TransferData from suc [%s] err: %v\n", node.name(), suc.name(), err)
		return false
	}

	node.setOnline(true)
	node.maintain()
	// node.stabilize()
	// time.Sleep(200 * time.Millisecond)
	return true
}

func (node *ChordNode) fixFinger() {
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
		res += fmt.Sprintf("[%s] ", (*sucList)[i].name())
	}
	return res
}

func (node *ChordNode) stabilize() {
	logrus.Infof("<stabilize> [%s] online: %v\n", node.name(), node.isOnline())

	suc := node.getOnlineSuccessor()
	logrus.Infof("<stabilize> [%s] online successor: [%s]\n", node.name(), suc.name())
	var newSuc NodeRecord
	RemoteCall(suc.Addr, "RPCNode.GetPredecessor", "", &newSuc.Addr)
	newSuc.ID = Hash(newSuc.Addr)
	logrus.Infof("<stabilize> newSucID %v node.ID %v suc.ID %v\n", newSuc, node.ID, suc.ID)
	if newSuc.Addr != "" && contains(newSuc.ID, node.ID, suc.ID) {
		suc = newSuc
	}
	var tmpList [successorListLen]NodeRecord
	err := RemoteCall(suc.Addr, "RPCNode.GetSuccessorList", "", &tmpList)
	if err != nil {
		logrus.Errorf("<stabilize> [%s] GetSuccessorList of [%s] err: %v\n", node.name(), suc.name(), err)
		return
	}
	logrus.Infof("<stabilize> [%s] suc [%s]'s sucList: %s\n", node.name(), suc.name(), sucListToString(&tmpList))
	node.fingerLock.Lock()
	node.finger[0] = suc
	node.fingerLock.Unlock()
	node.successorLock.Lock()
	node.successorList[0] = suc
	for i := 1; i < successorListLen; i++ {
		node.successorList[i] = tmpList[i-1]
	}
	node.successorLock.Unlock()
	logrus.Infof("<stabilize> [%s] will notify [%s]\n", node.name(), suc.name())
	err = RemoteCall(suc.Addr, "RPCNode.Notify", node.Addr, nil)
	if err != nil {
		logrus.Errorf("<stabilize> [%s] notify [%s] err: %v\n", node.name(), suc.name(), err)
	}
}

func (node *ChordNode) Stabilize(_ string, _ *string) error {
	node.stabilize()
	return nil
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
	logrus.Infof("<Notify> [%s] newPre [%s]\n", node.name(), getPortFromIP(newPre))
	pre := node.getPredecessor()
	if node.Ping(newPre) && (pre.Addr == "" || contains(Hash(newPre), pre.ID, node.ID)) {
		logrus.Infof("<Notify> [%s] set predecessor to [%s]\n", node.name(), newPre)
		node.setPredecessor(newPre)
		newPreData := make(map[string]string)
		err := RemoteCall(newPre, "RPCNode.GetData", "", &newPreData)
		if err != nil {
			logrus.Errorf("<Notify> [%s] get data from [%s] err: %v\n", node.name(), newPre, err)
			return err
		}
		node.backup = newPreData
		// node.addToBackup(newPreData) // TODO: remove from node.data?
	}
	return nil
}

func (node *ChordNode) mergeBackupToData() {
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

func (node *ChordNode) backupDataToSuccessor() {
	suc := node.getOnlineSuccessor()
	RemoteCall(suc.Addr, "RPCNode.AddToBackup", node.data, nil)
}

func (node *ChordNode) checkPredecessor() {
	pre := node.getPredecessor()
	// logrus.Infof("<checkPredecessor> [%s] pre [%s] ping %v\n", node.name(), pre.name(), node.Ping(pre.Addr))
	if pre.Addr != "" && !node.Ping(pre.Addr) {
		logrus.Warnf("<checkPredecessor> [%s] fail\n", node.Addr)
		node.setPredecessor("")
		node.mergeBackupToData()
		node.backupDataToSuccessor()
	}
}

func (node *ChordNode) CheckPredecessor(_ string, _ *string) error {
	node.checkPredecessor()
	return nil
}

func (node *ChordNode) maintain() {
	go func() {
		for node.isOnline() {
			node.fixFinger()
			time.Sleep(fixFingerInterval)
		}
	}()
	go func() {
		for node.isOnline() {
			node.stabilize()
			time.Sleep(stabilizeInterval)
		}
	}()
	go func() {
		for node.isOnline() {
			node.checkPredecessor()
			time.Sleep(checkPredecessorInterval)
		}
	}()
}

func (node *ChordNode) clear() {
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
func (node *ChordNode) Quit() {
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
	err := RemoteCall(suc.Addr, "RPCNode.CheckPredecessor", "", nil)
	if err != nil {
		logrus.Errorf("<Quit> [%s] call [%s] CheckPredecessor err: %v\n", node.name(), suc.name(), err)
	}
	pre := node.getPredecessor()
	err = RemoteCall(pre.Addr, "RPCNode.Stabilize", "", nil)
	if err != nil {
		logrus.Errorf("<Quit> [%s] call [%s] Stabilize err: %v\n", node.name(), pre.name(), err)
	}
	node.clear()
	// time.Sleep(300 * time.Millisecond)
}

// Chord offers a way of "normal" quitting.
// For "force quit", the node quit the network without informing other nodes.
// "ForceQuit" will be checked by TA manually.
func (node *ChordNode) ForceQuit() {
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
func (node *ChordNode) Ping(addr string) bool {
	if addr == "" {
		logrus.Warnf("<Ping> [%s] ping [%s] err: empty address\n", node.name(), getPortFromIP(addr))
		return false
	}
	if addr == node.Addr {
		return true
	}
	for i := 0; i < 3; i++ {
		conn, err := net.DialTimeout("tcp", addr, pingTimeout)
		if err == nil {
			conn.Close()
			return true
		} else {
			pc, _, _, _ := runtime.Caller(1)
			details := runtime.FuncForPC(pc)
			logrus.Warnf("<Ping> [%s] ping [%s] err: %v called by %s\n", node.name(), getPortFromIP(addr), err, details.Name())
			if strings.Contains(err.Error(), "refused") {
				return false
			}
			time.Sleep(1500 * time.Millisecond)
		}
		// ch := make(chan error)
		// go func() {
		// 	conn, err := net.Dial("tcp", addr)
		// 	if conn != nil {
		// 		conn.Close()
		// 	}
		// 	ch <- err
		// }()
		// select {
		// case err := <-ch:
		// 	if err == nil {
		// 		return true
		// 	} else {
		// 		pc, _, _, _ := runtime.Caller(1)
		// 		details := runtime.FuncForPC(pc)
		// 		logrus.Warnf("<Ping> [%s] ping [%s] err: %v called by %s\n", node.name(), getPortFromIP(addr), err, details.Name())
		// 		continue
		// 	}
		// case <-time.After(pingTimeout):
		// 	continue
		// }
	}
	return false
}

type FindSucInput struct {
	ID    *big.Int
	Depth int
}

func (node *ChordNode) FindSuccessor(input FindSucInput, result *string) error {
	id := input.ID
	if !node.isOnline() {
		return fmt.Errorf("<FindSuccessor> [%s] offline\n", node.name())
	}
	// logrus.Infof("<FindSuccessor> [%s] find %s\n", node.name(), id.String())
	suc := node.getOnlineSuccessor()
	if suc.Addr == "" {
		return fmt.Errorf("Cannot find successor of [%s]", node.Addr)
	}
	logrus.Infof("<FindSuccessor> [%s] depth: %d, online successor: [%s]\n", node.name(), input.Depth, suc.name())
	// logrus.Infof("<FindSuccessor> [%s] target: %s self: %s suc: %s contains:%v\n", node.name(), id, node.ID, suc.ID.String(), contains(id, node.ID, suc.ID))

	if id.Cmp(suc.ID) == 0 || contains(id, node.ID, suc.ID) {
		*result = suc.Addr
		logrus.Infof("<FindSuccessor> [%s] depth: %d found successor [%s] is target, id: %s self: %s suc: %s\n", node.name(), input.Depth, suc.name(), id, node.ID, suc.ID)
		return nil
	}
	p := node.closestPrecedingFinger(id)
	return RemoteCall(p, "RPCNode.FindSuccessor", FindSucInput{id, input.Depth + 1}, result)
}

func (node *ChordNode) getOnlineSuccessor() NodeRecord {
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

func (node *ChordNode) closestPrecedingFinger(id *big.Int) (addr string) {
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

func (node *ChordNode) PutValue(pair Pair, _ *string) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	suc := node.getOnlineSuccessor()
	err := RemoteCall(suc.Addr, "RPCNode.PutInBackup", pair, nil)
	if err != nil {
		logrus.Errorf("<PutValue> [%s] put in [%s]'s backup, err: %v\n", node.name(), suc.name(), err)
		return err
	}
	return nil
}

func (node *ChordNode) PutInBackup(pair Pair, _ *string) error {
	node.backupLock.Lock()
	node.backup[pair.Key] = pair.Value
	node.backupLock.Unlock()
	return nil
}

// Put a key-value pair into the network (if KEY is already in the network, cover it)
func (node *ChordNode) Put(key string, value string) bool {
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
	err = RemoteCall(target, "RPCNode.PutValue", Pair{key, value}, nil)
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
	err = RemoteCall(target, "RPCNode.GetValue", key, &value)
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
	err := RemoteCall(suc.Addr, "RPCNode.DeleteInBackup", key, nil)
	if err != nil {
		logrus.Errorf("<DeleteValue> delete in [%s]'s backup, err: %v\n", suc.name(), err)
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
	err = RemoteCall(target, "RPCNode.DeleteValue", key, nil)
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
