package dict

import "github.com/sunminx/RDB/internal/sds"

type MapDict struct {
	dict map[string]Robj
}

type RobjType int

const (
	UnknownType RobjType = iota
	ObjString
)

type Robj struct {
	_type RobjType
	val   any
}

func NewMap() *MapDict {
	return &MapDict{
		dict: make(map[string]Robj),
	}
}

func NewRobj(obj any) Robj {
	switch obj.(type) {
	case sds.SDS:
		return Robj{ObjString, obj}
	case []byte:
		return Robj{ObjString, sds.New(obj.([]byte))}
	default:
	}
	return Robj{UnknownType, nil}
}

func (o *Robj) Val() any {
	return o.val
}

func (o *Robj) Type() RobjType {
	return o._type
}

type Entry struct {
	key string
	val Robj
}

func (d *MapDict) Add(key string, val Robj) bool {
	_, ok := d.dict[key]
	if ok {
		return false
	}
	d.dict[key] = val
	return true
}

func (d *MapDict) Replace(key string, val Robj) bool {
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

func (d *MapDict) FetchValue(key string) (Robj, bool) {
	val, ok := d.dict[key]
	return val, ok
}

func (d *MapDict) GetRandomKey() Entry {
	return Entry{}
}

func (d *MapDict) Used() int {
	return len(d.dict)
}

func (d *MapDict) Size() int {
	return 0
}
