package db

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/sunminx/RDB/internal/hash"
	"github.com/sunminx/RDB/internal/list"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

const sdbNum = 2

const (
	// InNormalState indicates that nothing is in ongoing.
	InNormal int32 = 0

	// InPersistState indicates that there is currently a coroutine performing persistence.
	InPersist int32 = 1

	// InMergeState indicates that is merging sdbs[1] to sdbs[0].
	InMerge int32 = 2
)

type DB struct {
	// Usually, the last sdb is always empty and
	// will only be written when writing key-val during the redo of aof or rdb.
	sdbs [sdbNum]*sdb

	status atomic.Int32
}

const (
	// Redoing represents that aof or rdb is being redone.
	// During this period, the data is written to tmpdb (sdbs[len(sdbs)-1]).
	Redoing = (iota + 1) << 30
)

func New() *DB {
	return &DB{sdbs: [sdbNum]*sdb{newSdb(0), newSdb(1)}}
}

func deepcopy(val *obj.Robj) *obj.Robj {
	switch val.Type() {
	case obj.TypeString:
		return sds.DeepCopy(val)
	case obj.TypeList:
		return list.DeepCopy(val)
	case obj.TypeHash:
		return hash.DeepCopy(val)
	default:
		return nil
	}
}

func (db *DB) Get(key string) (*obj.Robj, bool) {
	status := db.status.Load()
	if status != InNormal {
		sdb := db.sdbs[1]
		if val := sdb.get(key); val != nil && !val.Deleted() {
			return val, true
		}
	}
	sdb := db.sdbs[0]
	val := sdb.get(key)
	return val, val != nil && !val.Deleted()
}

func (db *DB) Set(expire int64, key string, val *obj.Robj) {
	status := db.status.Load()
	if status == InPersist {
		sdb := db.sdbs[1]
		sdb.set(expire, key, val)
		return
	}
	if status == InMerge {
		sdb := db.sdbs[1]
		sdb.setDeleted(key)
	}
	sdb := db.sdbs[0]
	sdb.set(expire, key, val)
	return
}

func (db *DB) Del(key string) {
	status := db.status.Load()
	if status != InNormal {
		sdb := db.sdbs[1]
		sdb.setDeleted(key)
	}
	sdb := db.sdbs[0]
	sdb.setDeleted(key)
	return
}

func (db *DB) Expire(key string) time.Duration {
	status := db.status.Load()
	if status != InNormal {
		sdb := db.sdbs[1]
		if expire := sdb.expire(key); expire != -1 {
			return expire
		}
	}
	sdb := db.sdbs[0]
	return sdb.expire(key)
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

func (db *DB) SetStatus(s int32) {
	db.status.Store(s)
}

func (db *DB) InNormalStatus() bool {
	return db.isThatStatus(InNormal)
}

func (db *DB) InMergeStatus() bool {
	return db.isThatStatus(InMerge)
}

func (db *DB) isThatStatus(s int32) bool {
	return db.status.Load() == s
}

const dbMergeBatchNum = 1024

func (db *DB) MergeIfNeeded(timeout time.Duration) error {
	start := time.Now()
	cnt := 0

	for {
		if db.sdbs[1].isEmpty() {
			db.status.Store(InNormal)
			slog.Info("the merge of DB has finished")
			break
		}
		timeused := time.Since(start)
		if time.Since(start) >= timeout {
			slog.Info(fmt.Sprintf("in db merge stage, %d key-val pair "+
				"had merged, timecost: %v\n", cnt, timeused))
			break
		}

		num := 0
		for e := range db.sdbs[1].Iterator() {
			k, v := e.Key, e.Val
			if !v.Deleted() {
				db.sdbs[0].set(-1, k, v)
			} else {
				if v := db.sdbs[0].get(k); v != nil && v.Deleted() {
					db.sdbs[0].del(k)
				}
			}
			db.sdbs[1].del(k)
			num++
			if num == dbMergeBatchNum {
				break
			}
		}
		cnt += num
	}
	return nil
}

type IterCallback func(context.Context, DBEntry) error

// Iter iterate db while calling cb for every entry.
func (db *DB) Iter(ctx context.Context, cb IterCallback, mode int) error {
	for e := range db.sdbs[0].Iterator() {
		if err := cb(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Empty() int {
	db.status.Store(InNormal)
	n := db.sdbs[0].empty()
	m := db.sdbs[1].empty()
	return n + m
}
