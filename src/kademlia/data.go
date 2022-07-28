package kademlia

import (
	"sync"
	"time"
)

const (
	// RepublishTime = 86400 * time.Second // the time after which the original publisher must republish a key/value pair
	// ExpireTime    = 86400 * time.Second // the time after which a key/value pair expires; this is a time-to-live (TTL) from the original publication date
	// RefreshTime   = 3600 * time.Second  // after which an otherwise unaccessed bucket must be refreshed
	// ReplicateTime = 3600 * time.Second  // the interval between Kademlia replication events, when a node is required to publish its entire database

	RepublishTime = 10 * time.Second // the time after which the original publisher must republish a key/value pair
	ExpireTime    = 12 * time.Second // the time after which a key/value pair expires; this is a time-to-live (TTL) from the original publication date
	RefreshTime   = 5 * time.Second  // after which an otherwise unaccessed bucket must be refreshed
	ReplicateTime = 5 * time.Second  // the interval between Kademlia replication events, when a node is required to publish its entire database
)

type DataType struct {
	data          map[string]string
	expireTime    map[string]time.Time
	republishTime map[string]time.Time
	mu            sync.RWMutex
}

func (data *DataType) Init() {
	data.data = make(map[string]string)
	data.expireTime = make(map[string]time.Time)
	data.republishTime = make(map[string]time.Time)
}

func (data *DataType) RepublishList() (republishList []string) {
	data.mu.RLock()
	for key, t := range data.republishTime {
		if time.Now().After(t) {
			republishList = append(republishList, key)
		}
	}
	data.mu.RUnlock()
	return
}

func (data *DataType) Expire() {
	var expiredKeys []string

	data.mu.RLock()
	for key, t := range data.expireTime {
		if time.Now().After(t) {
			expiredKeys = append(expiredKeys, key)
		}
	}
	data.mu.RUnlock()

	data.mu.Lock()
	for _, key := range expiredKeys {
		delete(data.data, key)
		delete(data.expireTime, key)
		delete(data.republishTime, key)
	}
	data.mu.Unlock()
}

func (data *DataType) Store(key string, val string) {
	data.mu.Lock()
	defer data.mu.Unlock()
	data.data[key] = val
	data.expireTime[key] = time.Now().Add(ExpireTime)
	data.republishTime[key] = time.Now().Add(RepublishTime)
}

func (data DataType) Load(key string) (ret string, ok bool) {
	data.mu.RLock()
	defer data.mu.RUnlock()
	ret, ok = data.data[key]
	return
}

func (data *DataType) Delete(key string) {
	data.mu.Lock()
	defer data.mu.Unlock()
	delete(data.data, key)
	delete(data.expireTime, key)
	delete(data.republishTime, key)
}

func (data *DataType) GetData() map[string]string {
	data.mu.RLock()
	defer data.mu.RUnlock()
	return data.data
}
