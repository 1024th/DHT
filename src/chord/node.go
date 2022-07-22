package chord

import (
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	successorListLen = 5
)

const (
	checkPredecessorInterval = 200 * time.Millisecond
	stabilizeInterval        = 150 * time.Millisecond
	fixFingerInterval        = 200 * time.Millisecond
	pingTimeout              = 300 * time.Millisecond
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
	Key   string
	Value string
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
	node.clear()
	node.server = rpc.NewServer()
	node.server.Register(node)
}

func (node *ChordNode) PrintSelf() {
	info := fmt.Sprintf("I'm [%s], online: %v, predecessor: [%s], successors:", node.name(), node.isOnline(), node.predecessor.name())
	for i := 0; i < successorListLen; i++ {
		info += fmt.Sprintf("[%s] ", node.successorList[i].name())
	}
	info += "\n"
	logrus.Info(info)
}

func (node *ChordNode) Serve() {
	logrus.Infof("<Serve> [%s] start serving...\n", node.name())
	count := 0
	var countLock sync.Mutex
	// defer func() {
	// 	node.listener.Close()
	// }()
	for node.isOnline() {
		conn, err := node.listener.Accept()
		// logrus.Infof("<Serve> [%s] accept\n", node.name())
		if !node.isOnline() {
			logrus.Warnf("<Serve> [%s] offline, stop serving...\n", node.Addr)
			break
		}

		if err != nil {
			logrus.Errorf("<Serve> [%s], listener accept error, stop serving.", node.Addr)
			return
		}
		countLock.Lock()
		count++
		countLock.Unlock()
		// logrus.Infof("<Serve> [%s] start connect num: %d\n", node.name(), count)
		go func() {
			node.server.ServeConn(conn)
			countLock.Lock()
			count--
			countLock.Unlock()
			// logrus.Infof("<Serve> [%s] end connect num: %d\n", node.name(), count)
		}()
		// go node.server.ServeConn(conn)
	}
	logrus.Warnf("<Serve> [%s] stop serving.\n", node.Addr)
}

func (node *ChordNode) Hello(_ string, reply *string) error {
	*reply = fmt.Sprintf("\"Hello!\", said by node [%s].\n", node.name())
	logrus.Infoln(*reply)
	return nil
}

// "Run" is called after calling "NewNode".
func (node *ChordNode) Run() {
	// logrus.Tracef("<Run> [%s]\n", node.name())
	node.setOnline()
	var err error
	node.listener, err = net.Listen("tcp", node.Addr)
	if err != nil {
		logrus.Errorf("<Run> [%s] listen err: %v\n", node.name(), err)
	}
	go node.Serve()
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
	err := RemoteCall(suc.Addr, "ChordNode.ShrinkBackup", *preData, nil)
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
	var suc NodeRecord
	err := RemoteCall(addr, "ChordNode.FindSuccessor", Hash(addr), &suc.Addr)
	if err != nil {
		logrus.Errorf("<Join> [%s] call [%s].FindSuccessor err: %s\n", node.name(), addr, err)
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

	// for i := 1; i < hashLength; i++ {
	// 	start := hashAdd(node.ID, 1<<i)
	// 	var fingerAddr string
	// 	err = RemoteCall(suc.Addr, "ChordNode.FindSuccessor", start, &fingerAddr)
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
	err = RemoteCall(suc.Addr, "ChordNode.TransferData", node.Addr, &node.data)
	node.dataLock.Unlock()
	if err != nil {
		logrus.Errorf("<Join> [%s] TransferData from suc [%s] err: %v\n", node.name(), suc.name(), err)
		return false
	}

	node.setOnline()
	node.maintain()
	// node.stabilize()
	// time.Sleep(10 * time.Millisecond)
	return true
}

func (node *ChordNode) fixFinger() {
	t := hashAdd(node.ID, 1<<node.curFinger)
	var res string
	err := node.FindSuccessor(t, &res)
	if err != nil {
		logrus.Errorf("<fixFinger> [%s] Find Successor of %v err: %v\n", node.name(), t, err)
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
		res += fmt.Sprintf("[%s] ", (*sucList)[i].name())
	}
	return res
}

func (node *ChordNode) stabilize() {
	logrus.Infof("<stabilize> [%s] online: %v\n", node.name(), node.isOnline())

	suc := node.getOnlineSuccessor()
	logrus.Infof("<stabilize> [%s] online successor: [%s]\n", node.name(), suc.name())
	var newSuc NodeRecord
	RemoteCall(suc.Addr, "ChordNode.GetPredecessor", "", &newSuc.Addr)
	newSuc.ID = Hash(newSuc.Addr)
	logrus.Infof("<stabilize> newSucID %v node.ID %v suc.ID %v\n", newSuc, node.ID, suc.ID)
	if newSuc.Addr != "" && node.Ping(newSuc.Addr) && contains(newSuc.ID, node.ID, suc.ID) {
		suc = newSuc
	}
	var tmpList [successorListLen]NodeRecord
	err := RemoteCall(suc.Addr, "ChordNode.GetSuccessorList", "", &tmpList)
	if err != nil {
		logrus.Infof("<stabilize> [%s] GetSuccessorList of [%s] err: %v\n", node.name(), suc.name(), err)
	}
	logrus.Infof("<stabilize> [%s] suc [%s]'s sucList: %s\n", node.name(), suc.name(), sucListToString(&tmpList))
	node.fingerLock.Lock()
	node.finger[0] = suc
	node.fingerLock.Unlock()
	node.successorLock.Lock()
	node.successorList[0] = suc
	for i := 1; i < successorListLen; i++ {
		if node.Ping(tmpList[i-1].Addr) {
			node.successorList[i] = tmpList[i-1]
		}
	}
	node.successorLock.Unlock()
	logrus.Infof("<stabilize> [%s] will notify [%s]\n", node.name(), suc.name())
	err = RemoteCall(suc.Addr, "ChordNode.Notify", node.Addr, nil)
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
		err := RemoteCall(newPre, "ChordNode.GetData", "", &newPreData)
		if err != nil {
			logrus.Errorf("<Notify> [%s] get data from [%s] err: %v\n", node.name(), newPre, err)
			return err
		}
		node.addToBackup(newPreData) // TODO: remove from node.data?
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
	RemoteCall(suc.Addr, "ChordNode.AddToBackup", node.data, nil)
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
	node.setOffline()
	node.listener.Close()
	suc := node.getOnlineSuccessor()
	err := RemoteCall(suc.Addr, "ChordNode.CheckPredecessor", "", nil)
	if err != nil {
		logrus.Errorf("<Quit> [%s] call [%s] CheckPredecessor err: %v\n", node.name(), suc.name(), err)
	}
	pre := node.getPredecessor()
	err = RemoteCall(pre.Addr, "ChordNode.Stabilize", "", nil)
	if err != nil {
		logrus.Errorf("<Quit> [%s] call [%s] Stabilize err: %v\n", node.name(), pre.name(), err)
	}
	node.clear()
	time.Sleep(300 * time.Millisecond)
}

// Chord offers a way of "normal" quitting.
// For "force quit", the node quit the network without informing other nodes.
// "ForceQuit" will be checked by TA manually.
func (node *ChordNode) ForceQuit() {}

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
		// conn, err := net.DialTimeout("tcp", addr, timeout)
		ch := make(chan bool)
		go func() {
			conn, err := net.Dial("tcp", addr)
			if conn != nil {
				conn.Close()
			}
			if err == nil {
				ch <- true
			} else {
				logrus.Warnf("<Ping> [%s] ping [%s] err: %v\n", node.name(), addr, err)
				ch <- false
			}
		}()
		select {
		case ok := <-ch:
			if ok {
				return ok
			}
			continue
		case <-time.After(pingTimeout):
			continue
		}
	}
	return false
}

func (node *ChordNode) FindSuccessor(id *big.Int, result *string) error {
	if !node.isOnline() {
		return fmt.Errorf("<FindSuccessor> [%s] offline\n", node.name())
	}
	// logrus.Infof("<FindSuccessor> [%s] find %s\n", node.name(), id.String())
	suc := node.getOnlineSuccessor()
	if suc.Addr == "" {
		return fmt.Errorf("Cannot find successor of [%s]", node.Addr)
	}
	logrus.Infof("<FindSuccessor> [%s] online successor: [%s]\n", node.name(), suc.name())
	// logrus.Infof("<FindSuccessor> [%s] target: %s self: %s suc: %s contains:%v\n", node.name(), id, node.ID, suc.ID.String(), contains(id, node.ID, suc.ID))

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
			// 	RemoteCall(suc.Addr, "ChordNode.Notify", node.Addr, nil)
			// }
			return suc
		}
	}
	logrus.Errorf("<getOnlineSuccessor> cannot find successor of [%s]\n", node.Addr)
	return NodeRecord{}
}

func (node *ChordNode) closestPrecedingFinger(id *big.Int) (addr string) {
	for i := hashLength - 1; i >= 0; i-- {
		if node.finger[i].Addr != "" && !node.Ping(node.finger[i].Addr) {
			continue
		}
		if node.finger[i].ID != nil && contains(node.finger[i].ID, node.ID, id) {
			return node.finger[i].Addr
		}
	}

	return node.getOnlineSuccessor().Addr
}

func (node *ChordNode) PutValue(pair Pair, _ *string) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	suc := node.getOnlineSuccessor()
	err := RemoteCall(suc.Addr, "ChordNode.PutInBackup", pair, nil)
	if err != nil {
		logrus.Errorf("<DeleteValue> [%s] delete in [%s]'s backup, err: %v\n", node.name(), suc.name(), err)
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
	err := node.FindSuccessor(Hash(key), &target)
	if err != nil {
		logrus.Errorf("<Put> [%s] find successor of key %v err: %v\n", node.name(), key, err)
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
	if !node.isOnline() {
		logrus.Errorf("<Get> [%s] offline\n", node.Addr)
		return false, ""
	}
	var target string
	err := node.FindSuccessor(Hash(key), &target)
	if err != nil {
		logrus.Errorf("<Get> [%s] find successor of key %v err: %v\n", node.name(), key, err)
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
	err := node.FindSuccessor(Hash(key), &target)
	if err != nil {
		logrus.Errorf("<Delete> [%s] find successor of key %v err: %v\n", node.name(), key, err)
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

func (node NodeRecord) name() string {
	return getPortFromIP(node.Addr)
}

func getPortFromIP(ip string) string {
	return ip[strings.Index(ip, ":")+1:]
}
