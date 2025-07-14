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
	add(string, *obj.Robj) bool
	set(string, *obj.Robj) bool
	replace(string, *obj.Robj) bool
	del(string) bool
	fetchValue(string) (*obj.Robj, bool)
	getRandomKey() Entry
	used() int
	size() int
	iterator(context.Context) <-chan *Entry
	empty() int
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
	o, _ := sdb.dict.fetchValue(key)
	return o
}

func (sdb *sdb) expire(key string) int64 {
	e, ok := sdb.expires.fetchValue(key)
	if !ok {
		return -1
	}
	return e.Val().(int64)
}

func (sdb *sdb) keyIsExpired(key string) bool {
	v, ok := sdb.expires.fetchValue(key)
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
		sdb.dict.del(key)
		return false
	}

	if val != nil {
		sds.TryObjectEncoding(val)
		added := sdb.dict.set(key, val)
		if added {
			sdb.slen++
		}
	}

	if expire != -1 {
		_ = sdb.expires.set(key, sds.NewRobj(expire))
	}
	return true
}

func (sdb *sdb) del(key string) {
	deled := sdb.dict.del(key)
	if deled {
		sdb.slen--
		sdb.expires.del(key)
	}
	return
}

func (sdb *sdb) setDeleted(key string) {
	val := sdb.get(key)
	val.SetDeleted(true)
	sdb.slen--
}

func (sdb *sdb) empty() int {
	n := sdb.dict.empty()
	sdb.expires.empty()
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
		for entry := range sdb.dict.iterator(ctx) {
			dbEntry := DBEntry{entry, -1}
			v, ok := sdb.expires.fetchValue(entry.Key)
			if ok {
				dbEntry.Expire = v.Val().(int64)
			}
			ch <- dbEntry
		}
	}()
	return ch
}
