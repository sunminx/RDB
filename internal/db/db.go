package db

import (
	"time"

	"github.com/sunminx/RDB/internal/dict"
)

type DB struct {
	sdb    []*sdb
	sdblen int
}

const dbcount = 16

func New() *DB {
	sdb := make([]*sdb, dbcount, dbcount)
	var i int
	for ; i < dbcount; i++ {
		sdb[i] = newSdb()
	}
	return &DB{sdb, i}
}

func (db *DB) lookupSdb(key string) *sdb {
	return db.sdb[0]
}

func (db *DB) LookupKeyRead(key string) (dict.Robj, bool) {
	return db.lookupKeyReadWithFlags(key)
}

func (db *DB) LookupKeyWrite(key string) (dict.Robj, bool) {
	// todo
	return db.lookupKey(key)
}

func (db *DB) lookupKey(key string) (dict.Robj, bool) {
	sdb := db.lookupSdb(key)
	sdb.RLock()
	defer sdb.RUnlock()
	val, ok := sdb.dict.FetchValue(key)
	if ok {
		// todo
		return val, true
	}

	return dict.Robj{}, false
}

var emptyRobj = dict.Robj{}

func (db *DB) lookupKeyReadWithFlags(key string) (dict.Robj, bool) {
	sdb := db.lookupSdb(key)
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

func (db *DB) expireIfNeeded(key string) bool {
	sdb := db.lookupSdb(key)
	if !sdb.keyIsExpired(key) {
		return false
	}
	return sdb.syncDel(key)
}

func (db *DB) SetKey(key string, val dict.Robj) {
	sdb := db.lookupSdb(key)
	sdb.Lock()
	defer sdb.Unlock()
	_ = val.TryObjectEncoding()

	if _, ok := sdb.dict.FetchValue(key); ok {
		sdb.dict.Replace(key, val)
	} else {
		sdb.dict.Add(key, val)
	}
}

func (db *DB) SetExpire(key string, expire time.Duration) {
	sdb := db.lookupSdb(key)
	sdb.Lock()
	defer sdb.Unlock()
	sdb.expires.Replace(key, dict.NewRobj(int64(expire)))
}

func (db *DB) DelKey(key string) {
	sdb := db.lookupSdb(key)
	sdb.Lock()
	defer sdb.Unlock()
	sdb.dict.Del(key)
}

const (
	activeExpireCycleLookupsPerLoop = 20
)

func (db *DB) ActiveExpireCycle(timelimit time.Duration) {
	start := time.Now()
	exit := false
	for i := 0; i < db.sdblen; i++ {
		sdb := db.sdb[i]
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
	}
	return
}

func (db *DB) activeExpireCycleTryExpire(entry dict.Entry, now time.Time) bool {
	key := entry.Key()
	expire := entry.TimeDurationVal()
	// expired
	if now.UnixMilli() > int64(expire) {
		sdb := db.lookupSdb(key)
		return sdb.syncDel(key)
	}
	return false
}

func (db *DB) keyIsExpired(key string) bool {
	sdb := db.lookupSdb(key)
	v, ok := sdb.expires.FetchValue(key)
	if !ok {
		return false
	}
	expire, _ := v.Val().(int64)
	return (time.Now().UnixMilli() - expire) > 0
}

func (db *DB) syncDel(key string) bool {
	sdb := db.lookupSdb(key)
	if sdb.expires.Used() > 0 {
		_ = sdb.expires.Del(key)
	}
	return sdb.dict.Del(key)
}
