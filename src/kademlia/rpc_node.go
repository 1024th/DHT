package kademlia

type RPCNode struct {
	ptr *Node
}

type Void struct{}

func (node *RPCNode) Ping(sender string, _ *Void) error {
	node.ptr.Update(NewNodeRecord(sender))
	return nil
}

type FindNodeArg struct {
	Target IDType
	Sender string
}

func (node *RPCNode) FindNode(arg FindNodeArg, reply *OrderedNodeList) error {
	*reply = node.ptr.FindNode(arg.Target)
	node.ptr.Update(NewNodeRecord(arg.Sender))
	return nil
}

type FindValueArg struct {
	Key    string
	Sender string
}

type FindValueReply struct {
	List  OrderedNodeList
	Value string
}

func (node *RPCNode) FindValue(arg FindValueArg, reply *FindValueReply) error {
	*reply = node.ptr.FindValue(arg.Key)
	node.ptr.Update(NewNodeRecord(arg.Sender))
	return nil
}

type StoreArg struct {
	Key    string
	Value  string
	Sender string
}

func (node *RPCNode) Store(arg StoreArg, _ *struct{}) error {
	node.ptr.data.Store(arg.Key, arg.Value)
	node.ptr.Update(NewNodeRecord(arg.Sender))
	return nil
}
