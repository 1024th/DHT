package chord

type RPCNode struct {
	ptr *ChordNode
}

type Void struct{}

func (node *RPCNode) Hello(_ Void, reply *string) error {
	return node.ptr.Hello(reply)
}

func (node *RPCNode) GetSuccessorList(_ Void, result *[successorListLen]NodeRecord) error {
	return node.ptr.GetSuccessorList(result)
}

func (node *RPCNode) ShrinkBackup(removeFromBak map[string]string, _ *Void) error {
	return node.ptr.ShrinkBackup(removeFromBak)
}

func (node *RPCNode) TransferData(preAddr string, preData *map[string]string) error {
	return node.ptr.TransferData(preAddr, preData)
}

func (node *RPCNode) GetPredecessor(_ Void, addr *string) error {
	return node.ptr.GetPredecessor(addr)
}

func (node *RPCNode) Stabilize(_ Void, _ *Void) error {
	return node.ptr.Stabilize()
}

func (node *RPCNode) AddToBackup(preData map[string]string, _ *Void) error {
	return node.ptr.AddToBackup(preData)
}

func (node *RPCNode) GetData(_ Void, res *map[string]string) error {
	return node.ptr.GetData(res)
}

func (node *RPCNode) Notify(newPre string, _ *Void) error {
	return node.ptr.Notify(newPre)
}

func (node *RPCNode) CheckPredecessor(Void, *Void) error {
	return node.ptr.CheckPredecessor()
}

func (node *RPCNode) FindSuccessor(input FindSucInput, result *string) error {
	return node.ptr.FindSuccessor(input, result)
}

func (node *RPCNode) PutValue(pair Pair, _ *Void) error {
	return node.ptr.PutValue(pair)
}

func (node *RPCNode) PutInBackup(pair Pair, _ *Void) error {
	return node.ptr.PutInBackup(pair)
}

func (node *RPCNode) GetValue(key string, value *string) error {
	return node.ptr.GetValue(key, value)
}

func (node *RPCNode) DeleteValue(key string, _ *Void) error {
	return node.ptr.DeleteValue(key)
}

func (node *RPCNode) DeleteInBackup(key string, _ *Void) error {
	return node.ptr.DeleteInBackup(key)
}
