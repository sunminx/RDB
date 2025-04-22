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

func (sdb *sdb) LookupKeyRead(key string) (dict.Robj, bool) {
	return sdb.lookupKeyReadWithFlags(key)
}

func (sdb *sdb) LookupKeyWrite(key string) (dict.Robj, bool) {
	// todo
	return sdb.lookupKey(key)
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

func (sdb *sdb) SetKey(key string, val dict.Robj) {
	sdb.Lock()
	defer sdb.Unlock()
	_ = val.TryObjectEncoding()

	if _, ok := sdb.dict.FetchValue(key); ok {
		sdb.dict.Replace(key, val)
	} else {
		sdb.dict.Add(key, val)
	}
}

func (sdb *sdb) SetExpire(key string, expire time.Duration) {
	sdb.Lock()
	defer sdb.Unlock()
	sdb.expires.Replace(key, dict.NewRobj(int64(expire)))
}

func (sdb *sdb) DelKey(key string) {
	sdb.Lock()
	defer sdb.Unlock()
	sdb.dict.Del(key)
}

func (sdb *sdb) ActiveExpireCycle(timelimit time.Duration) {
	start := time.Now()
	exit := false
	for iteration := 0; !exit; iteration++ {
		expired := 0
		n := sdb.expires.Used()
		if n > activeExpireCycleLookupsPerLoop {
			n = activeExpireCycleLookupsPerLoop
		}

		for ; n > 0; n-- {
			e := sdb.expires.GetRandomKey()
			if sdb.activeExpireCycleTryExpire(e, time.Now()) {
				expired += 1
			}
		}

		if iteration%16 == 0 {
			elapsed := time.Now().Sub(start)
			if elapsed > timelimit {
				exit = true
			}
		}

		if expired < activeExpireCycleLookupsPerLoop/4 {
			exit = true
		}
	}
	return
}

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
