package dict

import (
	"crypto/rand"
	"encoding/binary"
	"time"

	obj "github.com/sunminx/RDB/internal/object"
)

type MapDict struct {
	dict map[string]obj.Robj
}

func NewMap() *MapDict {
	return &MapDict{
		dict: make(map[string]obj.Robj),
	}
}

type Entry struct {
	key string
	val obj.Robj
}

func (e *Entry) Key() string {
	return e.key
}

func (e *Entry) TimeDurationVal() time.Duration {
	t, ok := e.val.Val().(int64)
	if !ok {
		return time.Duration(0)
	}
	return time.Duration(t)
}

func (d *MapDict) Add(key string, val obj.Robj) bool {
	_, ok := d.dict[key]
	if ok {
		return false
	}
	d.dict[key] = val
	return true
}

func (d *MapDict) Replace(key string, val obj.Robj) bool {
	d.dict[key] = val
	return true
}

func (d *MapDict) Del(key string) bool {
	_, ok := d.dict[key]
	if !ok {
		return false
	}
	delete(d.dict, key)
	return true
}

func (d *MapDict) FetchValue(key string) (obj.Robj, bool) {
	val, ok := d.dict[key]
	return val, ok
}

var emptyEntry = Entry{}

func (d *MapDict) GetRandomKey() Entry {
	times := random() % d.Used()
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

func (d *MapDict) Used() int {
	return len(d.dict)
}

func (d *MapDict) Size() int {
	return 0
}
