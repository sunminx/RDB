package db

import (
	"context"
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
	Set(string, *obj.Robj) bool
	Replace(string, *obj.Robj) bool
	Del(string) bool
	FetchValue(string) (*obj.Robj, bool)
	GetRandomKey() Entry
	Used() int
	Size() int
	Iterator(context.Context) <-chan *Entry
	Empty() int
}

func newSdb(id int) *sdb {
	return &sdb{id: id, dict: NewMap(), expires: NewMap(), slen: 0}
}

var emptyRobj = obj.Robj{}

func (sdb *sdb) get(key string) *obj.Robj {
	if sdb.keyIsExpired(key) {
		sdb.del(key)
		return nil
	}
	o, _ := sdb.dict.FetchValue(key)
	return o
}

func (sdb *sdb) expire(key string) int64 {
	e, ok := sdb.expires.FetchValue(key)
	if !ok {
		return -1
	}
	return e.Val().(int64)
}

func (sdb *sdb) keyIsExpired(key string) bool {
	v, ok := sdb.expires.FetchValue(key)
	if !ok {
		return false
	}
	expire, _ := v.Val().(int64)
	return time.Now().UnixMilli()-expire > 0
}

func (sdb *sdb) set(expire int64, key string, val *obj.Robj) bool {
	// Check timestamp if greater than time now. If not, we should delete key instantly.
	// The value of expire is -1 that means timestamp is not be setted, so we just ignore that test.
	if expire != -1 && time.Now().UnixMilli()-expire > 0 {
		sdb.dict.Del(key)
		return false
	}

	if val != nil {
		sds.TryObjectEncoding(val)
		added := sdb.dict.Set(key, val)
		if added {
			sdb.slen++
		}
	}

	if expire != -1 {
		_ = sdb.expires.Set(key, sds.NewRobj(expire))
	}
	return true
}

func (sdb *sdb) del(key string) {
	deled := sdb.dict.Del(key)
	if deled {
		sdb.slen--
		sdb.expires.Del(key)
	}
	return
}

func (sdb *sdb) setDeleted(key string) {
	val := sdb.get(key)
	val.SetDeleted(true)
	sdb.slen--
}

func (sdb *sdb) empty() int {
	n := sdb.dict.Empty()
	sdb.expires.Empty()
	sdb.slen = 0
	return n
}

func (sdb *sdb) isEmpty() bool {
	return sdb.slen == 0
}

const (
	activeExpireCycleLookupsPerLoop = 20
)

func (sdb *sdb) activeExpireCycleTryExpire(entry Entry, now time.Time) bool {
	expire := entry.TimeDurationVal()
	// expired
	if now.UnixMilli() > int64(expire) {
		sdb.del(entry.Key)
		return true
	}
	return false
}

type DBEntry struct {
	*Entry
	Expire int64
}

func (sdb *sdb) Iterator(ctx context.Context) <-chan DBEntry {
	ch := make(chan DBEntry)
	go func() {
		defer close(ch)
		for entry := range sdb.dict.Iterator(ctx) {
			dbEntry := DBEntry{entry, -1}
			v, ok := sdb.expires.FetchValue(entry.Key)
			if ok {
				dbEntry.Expire = v.Val().(int64)
			}
			ch <- dbEntry
		}
	}()
	return ch
}
