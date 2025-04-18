package dict

import (
	"crypto/rand"
	"encoding/binary"
	"strconv"
	"time"

	"github.com/sunminx/RDB/internal/sds"
)

type MapDict struct {
	dict map[string]Robj
}

type RobjType int

const (
	UnknownType RobjType = iota
	ObjString
)

type EncodingType int

const (
	UnknownEncodingType EncodingType = iota
	ObjEncodingInt
	ObjEncodingRaw
)

type Robj struct {
	_type    RobjType
	encoding EncodingType
	val      any
}

func NewMap() *MapDict {
	return &MapDict{
		dict: make(map[string]Robj),
	}
}

func NewRobj(obj any) Robj {
	switch obj.(type) {
	case sds.SDS:
		return Robj{ObjString, ObjEncodingRaw, obj}
	case []byte:
		return Robj{ObjString, ObjEncodingRaw, sds.New(obj.([]byte))}
	case int64, time.Duration:
		return Robj{ObjString, ObjEncodingInt, obj}
	default:
	}
	return Robj{UnknownType, UnknownEncodingType, nil}
}

func (o *Robj) Val() any {
	return o.val
}

func (o *Robj) SetVal(val any) {
	o.val = val
}

func (o *Robj) Type() RobjType {
	return o._type
}

func (o *Robj) SDSEncodedObject() bool {
	return o.encoding == ObjEncodingRaw
}

func (o *Robj) TryObjectEncoding() error {
	if o._type != ObjString {
		return nil
	}
	if !o.SDSEncodedObject() {
		return nil
	}

	var numval int64
	var err error
	s := o.val.(sds.SDS)
	val := s.String()

	if len(val) <= 20 {
		if numval, err = strconv.ParseInt(val, 10, 64); err == nil {
			if o.encoding == ObjEncodingRaw {
				o.val = numval
				o.encoding = ObjEncodingInt
			}
		}
	}

	return nil
}

func (o *Robj) CheckType(_type RobjType) bool {
	return o._type == _type
}

func (o *Robj) CheckEncoding(encoding EncodingType) bool {
	return o.encoding == encoding
}

func (o *Robj) Int64Val() (int64, bool) {
	if o._type != ObjString {
		return 0, false
	}
	if o.SDSEncodedObject() {
		o.TryObjectEncoding()
	}
	if o.encoding == ObjEncodingInt {
		return o.val.(int64), true
	}
	return 0, false
}

type Entry struct {
	key string
	val Robj
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
