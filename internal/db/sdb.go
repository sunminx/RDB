package db

import (
	"time"

	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

type sdb struct {
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
	Iterator() <-chan *Entry
	Empty() int
}

func newSdb(id int) *sdb {
	return &sdb{id, NewMap(), NewMap(), 0}
}

func (sdb *sdb) lookupKey(key string) (*obj.Robj, bool) {
	val, ok := sdb.dict.FetchValue(key)
	if ok {
		// todo
		return val, true
	}

	return &emptyRobj, false
}

func (sdb *sdb) lookupKeyReadWithFlags(key string) (*obj.Robj, bool) {
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
	sds.TryObjectEncoding(val)

	if _, ok := sdb.dict.FetchValue(key); ok {
		sdb.dict.Replace(key, val)
	} else {
		sdb.dict.Add(key, val)
	}
}

func (sdb *sdb) setExpire(key string, expire time.Duration) {
	sdb.expires.Replace(key, sds.NewRobj(int64(expire)))
}

func (sdb *sdb) expire(key string) time.Duration {
	e, ok := sdb.expires.FetchValue(key)
	if !ok {
		return -1
	}
	return time.Duration(e.Val().(int64))
}

func (sdb *sdb) delKey(key string) {
	sdb.dict.Del(key)
	sdb.expires.Del(key)
}

const (
	activeExpireCycleLookupsPerLoop = 20
)

func (sdb *sdb) activeExpireCycleTryExpire(entry Entry, now time.Time) bool {
	expire := entry.TimeDurationVal()
	// expired
	if now.UnixMilli() > int64(expire) {
		return sdb.syncDel(entry.Key)
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

type DBEntry struct {
	*Entry
	Expire int64
}

func (sdb *sdb) Iterator() <-chan DBEntry {
	ch := make(chan DBEntry)
	go func() {
		defer close(ch)
		for entry := range sdb.dict.Iterator() {
			dbEntry := DBEntry{entry, -1}
			v, ok := sdb.expires.FetchValue(entry.Key)
			if !ok {
				dbEntry.Expire = v.Val().(int64)
			}
			ch <- dbEntry
		}
	}()
	return ch
}
