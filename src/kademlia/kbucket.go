package kademlia

import (
	"myrpc"
	"sync"
)

type KNodeRecordList struct {
	List [K]NodeRecord
	Size int
}

func (l *KNodeRecordList) Find(addr string) int {
	for i := 0; i < l.Size; i++ {
		if l.List[i].Addr == addr {
			return i
		}
	}
	return -1
}
func (l *KNodeRecordList) Remove(pos int) {
	for i := pos + 1; i < l.Size; i++ {
		l.List[i-1] = l.List[i]
	}
	l.Size--
}
func (l *KNodeRecordList) Insert(pos int, v NodeRecord) {
	for i := l.Size; i > pos; i-- {
		l.List[i] = l.List[i-1]
	}
	l.List[pos] = v
	l.Size++
}
func (l *KNodeRecordList) PushBack(v NodeRecord) {
	l.List[l.Size] = v
	l.Size++
}
func (l *KNodeRecordList) PopBack() {
	l.Size--
}
func (l *KNodeRecordList) MoveToBack(pos int) {
	tmp := l.List[pos]
	l.Remove(pos)
	l.PushBack(tmp)
}

type KBucketType struct {
	KNodeRecordList
	mu sync.Mutex
}

func (bucket *KBucketType) Update(node NodeRecord) {
	if node.Addr == "" {
		return
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	pos := bucket.Find(node.Addr)
	if pos != -1 {
		bucket.MoveToBack(pos)
	} else {
		if bucket.Size < K {
			bucket.PushBack(node)
		} else {
			if !myrpc.Ping(bucket.List[0].Addr) {
				bucket.Remove(0)
				bucket.PushBack(node)
			} else {
				bucket.MoveToBack(0)
			}
		}
	}
}

type OrderedNodeList struct {
	KNodeRecordList
	Target IDType
}

func (l *OrderedNodeList) Insert(node NodeRecord) (updated bool) {
	if !myrpc.Ping(node.Addr) {
		return false
	}
	found := l.Find(node.Addr)
	if found != -1 {
		return false
	}

	newDis := Xor(node.ID, l.Target)
	if l.Size < K {
		for i := 0; i < l.Size; i++ {
			oldDis := Xor(l.List[i].ID, l.Target)
			if newDis.LessThan(oldDis) {
				l.KNodeRecordList.Insert(i, node)
				return true
			}
		}
		l.PushBack(node)
		return true
	}
	for i := 0; i < K; i++ {
		oldDis := Xor(l.List[i].ID, l.Target)
		if newDis.LessThan(oldDis) {
			l.KNodeRecordList.PopBack()
			l.KNodeRecordList.Insert(i, node)
			return true
		}
	}
	return false
}

func (l *OrderedNodeList) Remove(node NodeRecord) (updated bool) {
	for i := 0; i < l.Size; i++ {
		if l.List[i].Addr == node.Addr {
			l.KNodeRecordList.Remove(i)
			return true
		}
	}
	return false
}
