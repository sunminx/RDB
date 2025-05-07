package db

import (
	"time"

	obj "github.com/sunminx/RDB/internal/object"
)

type DB struct {
	// Usually, the last sdb is always empty and
	// will only be written when writing key-val during the redo of aof or rdb.
	sdbs   sdbs
	sdblen int
}

const (
	// Redoing represents that aof or rdb is being redone.
	// During this period, the data is written to tmpdb (sdbs[len(sdbs)-1]).
	Redoing = (iota + 1) << 30
)

func New() *DB {
	sdb := make([]*sdb, 2, 2)
	var i int
	for ; i < 2; i++ {
		sdb[i] = newSdb(i)
	}
	return &DB{sdbs: sdb, sdblen: i}
}

func (db *DB) lookupSdb(key string) *sdb {
	return db.sdbs[0]
}

func (db *DB) LookupKeyRead(key string) (*obj.Robj, bool) {
	sdb := db.lookupSdb(key)
	return sdb.lookupKeyReadWithFlags(key)
}

func (db *DB) LookupKeyWrite(key string) (*obj.Robj, bool) {
	// todo
	sdb := db.lookupSdb(key)
	return sdb.lookupKey(key)
}

func (db *DB) SetKey(key string, val *obj.Robj) {
	sdb := db.lookupSdb(key)
	sdb.setKey(key, val)
	return
}

func (db *DB) SetExpire(key string, expire time.Duration) {
	sdb := db.lookupSdb(key)
	sdb.setExpire(key, expire)
	return
}

func (db *DB) DelKey(key string) {
	sdb := db.lookupSdb(key)
	sdb.delKey(key)
	return
}

func (db *DB) ActiveExpireCycle(timelimit time.Duration) {
	start := time.Now()
	exit := false
	for i := 0; i < db.sdblen; i++ {
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

func (db *DB) tryMergeSdb() error {
	return nil
}

func (db *DB) Iterator() <-chan Entry {
	// The data is always in Database 0.
	return db.sdbs[0].Iterator()
}
