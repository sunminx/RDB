package db

import (
	"sync"
	"time"

	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

type sdbs []*sdb

func (s sdbs) Len() int {
	return len(s)
}

func (s sdbs) Less(i, j int) bool {
	return s[i].slen < s[j].slen
}

func (s sdbs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type sdb struct {
	sync.RWMutex
	id      int
	dict    dictable
	expires dictable
	slen    int64
}

type dictable interface {
	Add(string, *obj.Robj) bool
	Replace(string, *obj.Robj) bool
	Del(string) bool
	FetchValue(string) (*obj.Robj, bool)
	GetRandomKey() Entry
	Used() int
	Size() int
}

func newSdb(id int) *sdb {
	return &sdb{sync.RWMutex{}, id, NewMap(), NewMap(), 0}
}

func (sdb *sdb) lookupKey(key string) (*obj.Robj, bool) {
	sdb.RLock()
	defer sdb.RUnlock()
	val, ok := sdb.dict.FetchValue(key)
	if ok {
		// todo
		return val, true
	}

	return &emptyRobj, false
}

func (sdb *sdb) lookupKeyReadWithFlags(key string) (*obj.Robj, bool) {
	sdb.Lock()
	defer sdb.Unlock()

	if sdb.expireIfNeeded(key) {
		return &emptyRobj, false
	}
	val, ok := sdb.dict.FetchValue(key)
	if ok {
		// todo
		return val, true
	}
	return &emptyRobj, false
}

func (sdb *sdb) expireIfNeeded(key string) bool {
	if !sdb.keyIsExpired(key) {
		return false
	}
	return sdb.syncDel(key)
}

var emptyRobj = obj.Robj{}

func (sdb *sdb) setKey(key string, val *obj.Robj) {
	sdb.Lock()
	defer sdb.Unlock()
	sds.TryObjectEncoding(val)

	if _, ok := sdb.dict.FetchValue(key); ok {
		sdb.dict.Replace(key, val)
	} else {
		sdb.dict.Add(key, val)
	}
}

func (sdb *sdb) setExpire(key string, expire time.Duration) {
	sdb.Lock()
	defer sdb.Unlock()
	sdb.expires.Replace(key, sds.NewRobj(int64(expire)))
}

func (sdb *sdb) delKey(key string) {
	sdb.Lock()
	defer sdb.Unlock()
	sdb.dict.Del(key)
}

const (
	activeExpireCycleLookupsPerLoop = 20
)

func (sdb *sdb) activeExpireCycleTryExpire(entry Entry, now time.Time) bool {
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
