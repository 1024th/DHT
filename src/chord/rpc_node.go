package chord

type RPCNode struct {
	ptr *ChordNode
}

func (node *RPCNode) Hello(_ string, reply *string) error {
	return node.ptr.Hello("", reply)
}

func (node *RPCNode) GetSuccessorList(_ string, result *[successorListLen]NodeRecord) error {
	return node.ptr.GetSuccessorList("", result)
}

func (node *RPCNode) ShrinkBackup(removeFromBak map[string]string, _ *string) error {
	return node.ptr.ShrinkBackup(removeFromBak, nil)
}

func (node *RPCNode) TransferData(preAddr string, preData *map[string]string) error {
	return node.ptr.TransferData(preAddr, preData)
}

func (node *RPCNode) GetPredecessor(_ string, addr *string) error {
	return node.ptr.GetPredecessor("", addr)
}

func (node *RPCNode) Stabilize(_ string, _ *string) error {
	return node.ptr.Stabilize("", nil)
}

func (node *RPCNode) AddToBackup(preData map[string]string, _ *string) error {
	return node.ptr.AddToBackup(preData, nil)
}

func (node *RPCNode) GetData(_ string, res *map[string]string) error {
	return node.ptr.GetData("", res)
}

func (node *RPCNode) Notify(newPre string, _ *string) error {
	return node.ptr.Notify(newPre, nil)
}

func (node *RPCNode) CheckPredecessor(_ string, _ *string) error {
	return node.ptr.CheckPredecessor("", nil)
}

func (node *RPCNode) FindSuccessor(input FindSucInput, result *string) error {
	return node.ptr.FindSuccessor(input, result)
}

func (node *RPCNode) PutValue(pair Pair, _ *string) error {
	return node.ptr.PutValue(pair, nil)
}

func (node *RPCNode) PutInBackup(pair Pair, _ *string) error {
	return node.ptr.PutInBackup(pair, nil)
}

func (node *RPCNode) GetValue(key string, value *string) error {
	return node.ptr.GetValue(key, value)
}

func (node *RPCNode) DeleteValue(key string, _ *string) error {
	return node.ptr.DeleteValue(key, nil)
}

func (node *RPCNode) DeleteInBackup(key string, _ *string) error {
	return node.ptr.DeleteInBackup(key, nil)
}
