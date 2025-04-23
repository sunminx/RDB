package db

import (
	"sync"
	"time"

	"github.com/sunminx/RDB/internal/dict"
)

type sdb struct {
	sync.RWMutex
	dict    dicter
	expires dicter
}

type dicter interface {
	Add(string, dict.Robj) bool
	Replace(string, dict.Robj) bool
	Del(string) bool
	FetchValue(string) (dict.Robj, bool)
	GetRandomKey() dict.Entry
	Used() int
	Size() int
}

func newSdb() *sdb {
	return &sdb{sync.RWMutex{}, dict.NewMap(), dict.NewMap()}
}

func (sdb *sdb) lookupKey(key string) (dict.Robj, bool) {
	sdb.RLock()
	defer sdb.RUnlock()
	val, ok := sdb.dict.FetchValue(key)
	if ok {
		// todo
		return val, true
	}

	return dict.Robj{}, false
}

func (sdb *sdb) lookupKeyReadWithFlags(key string) (dict.Robj, bool) {
	sdb.Lock()
	defer sdb.Unlock()

	if sdb.expireIfNeeded(key) {
		return emptyRobj, false
	}
	val, ok := sdb.dict.FetchValue(key)
	if ok {
		// todo
		return val, true
	}
	return emptyRobj, false
}

func (sdb *sdb) expireIfNeeded(key string) bool {
	if !sdb.keyIsExpired(key) {
		return false
	}
	return sdb.syncDel(key)
}

var emptyRobj = dict.Robj{}

func (sdb *sdb) setKey(key string, val dict.Robj) {
	sdb.Lock()
	defer sdb.Unlock()
	_ = val.TryObjectEncoding()

	if _, ok := sdb.dict.FetchValue(key); ok {
		sdb.dict.Replace(key, val)
	} else {
		sdb.dict.Add(key, val)
	}
}

func (sdb *sdb) setExpire(key string, expire time.Duration) {
	sdb.Lock()
	defer sdb.Unlock()
	sdb.expires.Replace(key, dict.NewRobj(int64(expire)))
}

func (sdb *sdb) delKey(key string) {
	sdb.Lock()
	defer sdb.Unlock()
	sdb.dict.Del(key)
}

const (
	activeExpireCycleLookupsPerLoop = 20
)

func (sdb *sdb) activeExpireCycleTryExpire(entry dict.Entry, now time.Time) bool {
	key := entry.Key()
	expire := entry.TimeDurationVal()
	// expired
	if now.UnixMilli() > int64(expire) {
		return sdb.syncDel(key)
	}
	return false
}

func (sdb *sdb) keyIsExpired(key string) bool {
	v, ok := sdb.expires.FetchValue(key)
	if !ok {
		return false
	}
	expire, _ := v.Val().(int64)
	return (time.Now().UnixMilli() - expire) > 0
}

func (sdb *sdb) syncDel(key string) bool {
	if sdb.expires.Used() > 0 {
		_ = sdb.expires.Del(key)
	}
	return sdb.dict.Del(key)
}
