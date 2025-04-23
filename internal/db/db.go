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
	sdb := db.lookupSdb(key)
	return sdb.lookupKeyReadWithFlags(key)
}

func (db *DB) LookupKeyWrite(key string) (dict.Robj, bool) {
	// todo
	sdb := db.lookupSdb(key)
	return sdb.lookupKey(key)
}

func (db *DB) SetKey(key string, val dict.Robj) {
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
