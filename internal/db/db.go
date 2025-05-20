package db

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/sunminx/RDB/internal/hash"
	"github.com/sunminx/RDB/internal/list"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

const sdbNum = 2

const (
	// InNormalState indicates that nothing is in ongoing.
	InNormalState uint8 = 0
	// InPersistState indicates that there is currently a coroutine performing persistence.
	InPersistState uint8 = 1
	// InMergeState indicates that is merging sdbs[1] to sdbs[0].
	InMergeState uint8 = 2
)

type DB struct {
	// Usually, the last sdb is always empty and
	// will only be written when writing key-val during the redo of aof or rdb.
	sdbs [sdbNum]*sdb

	// Protected by the server.CmdLock.
	state uint8
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
	_, val, ok := db.findForRead(key)
	return val, ok
}

func (db *DB) findForRead(key string) (*sdb, *obj.Robj, bool) {
	var sdb *sdb
	if db.state != InNormalState {
		sdb := db.sdbs[1]
		if val, ok := sdb.lookupKeyReadWithFlags(key); ok {
			return sdb, val, ok
		}
	}
	sdb = db.sdbs[0]
	val, ok := sdb.lookupKeyReadWithFlags(key)
	return sdb, val, ok
}

func (db *DB) LookupKeyWrite(key string) (*obj.Robj, bool) {
	_, val, ok := db.findForWrite(key)
	return val, ok
}

func (db *DB) findForWrite(key string) (*sdb, *obj.Robj, bool) {
	var sdb *sdb
	if db.state != InNormalState {
		sdb = db.sdbs[1]
		if val, ok := sdb.lookupKeyReadWithFlags(key); ok {
			if db.state == InMergeState {
				sdb.delKey(key)
				sdb = db.sdbs[0]
				sdb.setKey(key, val)

				// Try moving part key-val pair in sdbs[1] to sdbs[0].
				_ = db.MergeIfNeeded(20 * time.Millisecond)
			}
			return sdb, val, ok
		}
	}

	sdb = db.sdbs[0]
	val, ok := sdb.lookupKeyReadWithFlags(key)
	// during the persistence process, the key-val to sdbs[1].
	if ok && db.state == InPersistState {
		sdb = db.sdbs[1]
		val = deepcopy(val)
		sdb.setKey(key, val)
	}
	return sdb, val, ok
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

func (db *DB) SetKey(key string, val *obj.Robj) {
	_, robj, ok := db.findForWrite(key)
	if ok {
		robj.SetVal(val.Val())
		robj.SetType(val.Type())
		robj.SetEncoding(val.Encoding())
	} else {
		sdb := db.sdbs[0]
		if db.state == InPersistState {
			sdb = db.sdbs[1]
		}
		sdb.setKey(key, val)
	}
}

func (db *DB) SetExpire(key string, expire time.Duration) {
	sdb, _, ok := db.findForRead(key)
	if ok {
		sdb.setExpire(key, expire)
	}
}

func (db *DB) Expire(key string) time.Duration {
	sdb, _, ok := db.findForRead(key)
	if ok {
		return sdb.expire(key)
	}
	return -1
}

func (db *DB) DelKey(key string) {
	_, robj, ok := db.findForWrite(key)
	if ok {
		if db.state == InPersistState {
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

func (db *DB) SetState(state uint8) {
	db.state = state
}

func (db *DB) inMergeState() bool {
	return db.state == InMergeState
}

const dbMergeBatchNum = 128

func (db *DB) MergeIfNeeded(timeout time.Duration) error {
	if !db.inMergeState() {
		return nil
	}

	start := time.Now()
	cnt := 0
	for {
		if db.sdbs[1].slen == 0 {
			db.SetState(InNormalState)
			slog.Info("db merge has finished")
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
			db.sdbs[0].setKey(e.Key, e.Val)
			db.sdbs[1].delKey(e.Key)
			num++
			if num == dbMergeBatchNum {
				break
			}
		}
		cnt += num
	}
	return nil
}

func (db *DB) Iterator() <-chan DBEntry {
	// The data is always in Database 0.
	return db.sdbs[0].Iterator()
}
