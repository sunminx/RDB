package db

import (
	"sync"
	"time"

	"github.com/sunminx/RDB/internal/dict"
)

type DB struct {
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

func New() *DB {
	return &DB{sync.RWMutex{}, dict.NewMap(), dict.NewMap()}
}

func (db *DB) LookupKeyRead(key string) (dict.Robj, bool) {
	return db.lookupKeyReadWithFlags(key)
}

func (db *DB) LookupKeyWrite(key string) (dict.Robj, bool) {
	// todo
	return db.lookupKey(key)
}

func (db *DB) lookupKey(key string) (dict.Robj, bool) {
	db.RLock()
	defer db.RUnlock()
	val, ok := db.dict.FetchValue(key)
	if ok {
		// todo
		return val, true
	}

	return dict.Robj{}, false
}

var emptyRobj = dict.Robj{}

func (db *DB) lookupKeyReadWithFlags(key string) (dict.Robj, bool) {
	db.Lock()
	defer db.Unlock()

	if db.expireIfNeeded(key) {
		return emptyRobj, false
	}
	val, ok := db.dict.FetchValue(key)
	if ok {
		// todo
		return val, true
	}
	return emptyRobj, false
}

func (db *DB) expireIfNeeded(key string) bool {
	if !db.keyIsExpired(key) {
		return false
	}
	return db.syncDel(key)
}

func (db *DB) SetKey(key string, val dict.Robj) {
	db.Lock()
	defer db.Unlock()
	_ = val.TryObjectEncoding()

	if _, ok := db.dict.FetchValue(key); ok {
		db.dict.Replace(key, val)
	} else {
		db.dict.Add(key, val)
	}
}

func (db *DB) SetExpire(key string, expire time.Duration) {
	db.Lock()
	defer db.Unlock()
	db.expires.Replace(key, dict.NewRobj(int64(expire)))
}

func (db *DB) DelKey(key string) {
	db.Lock()
	defer db.Unlock()
	db.dict.Del(key)
}

const (
	activeExpireCycleLookupsPerLoop = 20
)

func (db *DB) ActiveExpireCycle(timelimit time.Duration) {
	start := time.Now()
	exit := false
	for iteration := 0; !exit; iteration++ {
		expired := 0
		n := db.expires.Used()
		if n > activeExpireCycleLookupsPerLoop {
			n = activeExpireCycleLookupsPerLoop
		}

		for ; n > 0; n-- {
			e := db.expires.GetRandomKey()
			if db.activeExpireCycleTryExpire(e, time.Now()) {
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

func (db *DB) activeExpireCycleTryExpire(entry dict.Entry, now time.Time) bool {
	key := entry.Key()
	expire := entry.TimeDurationVal()
	// expired
	if now.UnixMilli() > int64(expire) {
		return db.syncDel(key)
	}
	return false
}

func (db *DB) keyIsExpired(key string) bool {
	v, ok := db.expires.FetchValue(key)
	if !ok {
		return false
	}
	expire, _ := v.Val().(int64)
	return (time.Now().UnixMilli() - expire) > 0
}

func (db *DB) syncDel(key string) bool {
	if db.expires.Used() > 0 {
		_ = db.expires.Del(key)
	}
	return db.dict.Del(key)
}
