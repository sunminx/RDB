package db

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"time"

	obj "github.com/sunminx/RDB/internal/object"
)

type mapDict struct {
	dict map[string]*obj.Robj
}

func NewMap() *mapDict {
	return &mapDict{
		dict: make(map[string]*obj.Robj),
	}
}

type Entry struct {
	Key string
	Val *obj.Robj
}

func (e *Entry) TimeDurationVal() time.Duration {
	t, ok := e.Val.Val().(int64)
	if !ok {
		return time.Duration(0)
	}
	return time.Duration(t)
}

func (d *mapDict) set(key string, val *obj.Robj) bool {
	_, ok := d.dict[key]
	d.dict[key] = val
	return !ok
}

func (d *mapDict) add(key string, val *obj.Robj) bool {
	_, ok := d.dict[key]
	if ok {
		return false
	}
	d.dict[key] = val
	return true
}

func (d *mapDict) replace(key string, val *obj.Robj) bool {
	d.dict[key] = val
	return true
}

func (d *mapDict) del(key string) bool {
	_, ok := d.dict[key]
	if !ok {
		return false
	}
	delete(d.dict, key)
	return true
}

func (d *mapDict) fetchValue(key string) (*obj.Robj, bool) {
	val, ok := d.dict[key]
	return val, ok
}

var emptyEntry = Entry{}

func (d *mapDict) getRandomKey() Entry {
	times := random() % d.used()
	n := 0
	for key, val := range d.dict {
		if n == times {
			return Entry{key, val}
		}
		n++
	}
	return emptyEntry
}

func random() int {
	buf := make([]byte, 4)
	_, err := rand.Read(buf)
	if err != nil {
		return 0
	}
	n := int(binary.LittleEndian.Uint32(buf[:]))
	if n < 0 {
		return -n
	}
	return n
}

func (d *mapDict) used() int {
	return len(d.dict)
}

func (d *mapDict) size() int {
	return 0
}

func (d *mapDict) iterator(ctx context.Context) <-chan *Entry {
	ch := make(chan *Entry)
	go func() {
		defer close(ch)
		for k, v := range d.dict {
			select {
			case <-ctx.Done():
				return
			default:
			}
			ch <- &Entry{k, v}
		}
	}()
	return ch
}

func (d *mapDict) empty() int {
	ln := len(d.dict)
	d.dict = make(map[string]*obj.Robj)
	return ln
}
