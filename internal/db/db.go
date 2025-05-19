package db

import (
	"time"

	obj "github.com/sunminx/RDB/internal/object"
)

const sdbNum = 2

const (
	// InPersist indicates that there is currently a coroutine performing persistence.
	InPersist = true
	// NotInPersist indicates that persistence is not ongoing.
	NotInPersist = false
)

type DB struct {
	// Usually, the last sdb is always empty and
	// will only be written when writing key-val during the redo of aof or rdb.
	sdbs [sdbNum]*sdb

	// Protected by the server.CmdLock.
	persisting bool
}

const (
	// Redoing represents that aof or rdb is being redone.
	// During this period, the data is written to tmpdb (sdbs[len(sdbs)-1]).
	Redoing = (iota + 1) << 30
)

func New() *DB {
	return &DB{sdbs: [sdbNum]*sdb{newSdb(0), newSdb(1)}}
}

func (db *DB) LookupKeyRead(key string) (*obj.Robj, bool) {
	sdb := db.sdbs[0]
	return sdb.lookupKeyReadWithFlags(key)
}

func (db *DB) LookupKeyWrite(key string) (val *obj.Robj, ok bool) {
	var sdb *sdb
	if db.persisting {
		sdb = db.sdbs[1]
		val, ok = sdb.lookupKeyReadWithFlags(key)
		if ok {
			return
		}
	}

	sdb = db.sdbs[0]
	val, ok = sdb.lookupKeyReadWithFlags(key)
	// during the persistence process, the key-val to sdbs[1].
	if ok && db.persisting {
		sdb = db.sdbs[1]
		val = val.DeepCopy()
		sdb.setKey(key, val)
	}
	return val, ok
}

func (db *DB) SetKey(key string, val *obj.Robj) {
	robj, ok := db.LookupKeyWrite(key)
	if ok {
		robj.SetVal(val.Val())
		robj.SetType(val.Type())
		robj.SetEncoding(val.Encoding())
	} else {
		sdb := db.sdbs[0]
		if db.persisting {
			sdb = db.sdbs[1]
		}
		sdb.setKey(key, val)
	}
}

func (db *DB) SetExpire(key string, expire time.Duration) {
	sdb := db.sdbs[0]
	if db.persisting {
		sdb = db.sdbs[1]
	}
	_, ok := sdb.lookupKey(key)
	if ok {
		sdb.setExpire(key, expire)
	}
}

func (db *DB) Expire(key string) time.Duration {
	sdb := db.sdbs[0]
	if db.persisting {
		sdb = db.sdbs[1]
	}
	return sdb.expire(key)
}

func (db *DB) DelKey(key string) {
	robj, ok := db.LookupKeyWrite(key)
	if ok {
		if db.persisting {
			robj.SetDeleted(true)
		} else {
			sdb := db.sdbs[0]
			sdb.delKey(key)
		}
	}
}

func (db *DB) ActiveExpireCycle(timelimit time.Duration) {
	start := time.Now()
	exit := false
	for i := 0; i < sdbNum; i++ {
		sdb := db.sdbs[i]
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

func (db *DB) SetState(persisting bool) {
	db.persisting = persisting
}

func (db *DB) MergeIfNeeded() error {
	return nil
}

func (db *DB) Iterator() <-chan DBEntry {
	// The data is always in Database 0.
	return db.sdbs[0].Iterator()
}
