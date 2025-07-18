package db

import (
	"context"
	"time"

	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

type sdb struct {
	id      int
	dict    dict
	expires dict
	slen    int64
}

func newSdb(id int) *sdb {
	return &sdb{id: id, dict: newDict(), expires: newDict()}
}

var emptyRobj = obj.Robj{}

func (sdb *sdb) set(expires time.Duration, key string, val *obj.Robj) bool {
	// Check timestamp if greater than time now. If not, we should delete key instantly.
	// The value of expire is -1 that means timestamp is not be setted, so we just ignore that test.
	if expires != -1 && isPassed(expires, time.Now()) {
		sdb.dict.remove(key)
		return false
	}
	if val != nil {
		sds.TryObjectEncoding(val)
		added := sdb.dict.put(key, val)
		if added {
			sdb.slen++
		}
	}
	if expires != -1 {
		_ = sdb.expires.put(key, expires)
	}
	return true
}

func (sdb *sdb) get(key string) *obj.Robj {
	if sdb.isExpired(key) {
		sdb.del(key)
		return nil
	}
	o, ok := sdb.dict.get(key)
	if !ok {
		return nil
	}
	return o.(*obj.Robj)
}

func (sdb *sdb) getExpires(key string) time.Duration {
	expires, ok := sdb.expires.get(key)
	if !ok {
		return time.Duration(-1)
	}
	return expires.(time.Duration)
}

func (sdb *sdb) isExpired(key string) bool {
	v, ok := sdb.expires.get(key)
	if !ok {
		return false
	}
	expires := v.(time.Duration)
	return isPassed(expires, time.Now())
}

func isPassed(expires time.Duration, now time.Time) bool {
	return now.After(time.UnixMilli(int64(expires)))
}

func (sdb *sdb) del(key string) {
	ok := sdb.dict.remove(key)
	if ok {
		sdb.slen--
		_ = sdb.expires.remove(key)
	}
}

func (sdb *sdb) setDeleted(key string) {
	val := sdb.get(key)
	if val != nil {
		sdb.slen--
		val.SetDeleted(true)
	}
}

func (sdb *sdb) empty() int {
	n := sdb.dict.used()
	sdb.dict = newDict()
	sdb.expires = newDict()
	sdb.slen = 0
	return n
}

func (sdb *sdb) isEmpty() bool {
	return sdb.slen == 0
}

const (
	activeExpireCycleLookupsPerLoop = 20
)

func (sdb *sdb) activeExpireCycleTryExpire(key string, expires time.Duration, now time.Time) bool {
	if isPassed(expires, now) {
		sdb.del(key)
		return true
	}
	return false
}

func (sdb *sdb) iter(ctx context.Context) <-chan *Entry {
	c := make(chan *Entry)
	go func() {
		defer close(c)
		for key, robj := range sdb.dict {
			select {
			case <-ctx.Done():
				return
			default:
				entry := &Entry{Key: key, Val: robj.(*obj.Robj), Expires: time.Duration(-1)}
				expires, ok := sdb.expires.get(key)
				if ok {
					entry.Expires = expires.(time.Duration)
				}
				c <- entry
			}
		}
	}()
	return c
}
