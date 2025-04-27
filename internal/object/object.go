package object

import (
	"strconv"
	"time"

	"github.com/sunminx/RDB/internal/sds"
)

type RobjType int

const (
	UnknownType RobjType = iota
	ObjString
	ObjList
	ObjHash
)

type EncodingType int

const (
	UnknownEncodingType EncodingType = iota
	ObjEncodingInt
	ObjEncodingRaw
	ObjEncodingZiplist
	ObjEncodingQuicklist
)

type Robj struct {
	_type    RobjType
	encoding EncodingType
	val      any
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

func cond(c bool) int64 {
	if c {
		return 1
	}
	return 0
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
