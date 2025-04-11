package dict

import "github.com/sunminx/RDB/internal/sds"

type MapDict struct {
	dict map[string]Robj
}

type RobjType int

const (
	UnknownType RobjType = iota
	SdsType
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

func NewRobj(obj any) *Robj {
	switch obj.(type) {
	case *sds.SDS:
		return &Robj{SdsType, obj}
	default:
	}
	return &Robj{UnknownType, nil}
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
	if ok {
		return false
	}
	delete(d.dict, key)
	return true
}

func (d *MapDict) Get(key string) (Robj, bool) {
	val, ok := d.dict[key]
	return val, ok
}
