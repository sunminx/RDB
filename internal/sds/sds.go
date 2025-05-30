package sds

import (
	"strconv"

	"github.com/sunminx/RDB/internal/object"
	obj "github.com/sunminx/RDB/internal/object"
	. "github.com/sunminx/RDB/pkg/util"
)

type sds interface {
	Append(*obj.Robj, []byte)
	Len(*obj.Robj) int64
	Incr(*obj.Robj, int64) int64
}

func NewRobj(val any) *obj.Robj {
	robj := obj.Robj{}
	robj.SetType(obj.TypeString)
	switch val.(type) {
	case SDS:
		robj.SetEncoding(obj.EncodingRaw)
	case []byte:
		val = New(val.([]byte))
		robj.SetEncoding(obj.EncodingRaw)
	case int64:
		robj.SetEncoding(obj.EncodingInt)
	}
	robj.SetVal(val)
	return &robj
}

func DeepCopy(robj *obj.Robj) *obj.Robj {
	if robj.CheckEncoding(obj.EncodingRaw) {
		ns := unwrap(robj).deepcopy()
		return NewRobj(ns)
	}
	return nil
}

func Append(robj *obj.Robj, s []byte) {
	if robj.CheckEncoding(obj.EncodingRaw) {
		unwrap(robj).Cat(s)
	}
	return
}

func Len(robj *obj.Robj) int64 {
	if robj.CheckEncoding(obj.EncodingRaw) {
		return int64(unwrap(robj).Len())
	}
	if robj.CheckEncoding(obj.EncodingInt) {
		return Digit10(uint64(unwrapInt(robj)))
	}
	return 0
}

func Incr(robj *obj.Robj, n int64) int64 {
	_ = TryObjectEncoding(robj)
	if robj.CheckEncoding(obj.EncodingInt) {
		n += unwrapInt(robj)
		robj.SetVal(n)
		return n
	}
	return 0
}

func TryObjectEncoding(robj *obj.Robj) error {
	if !robj.CheckType(object.TypeString) {
		return nil
	}
	if !robj.CheckEncoding(obj.EncodingRaw) {
		return nil
	}

	var numval int64
	var err error
	val := unwrap(robj).String()
	if len(val) <= 20 {
		if numval, err = strconv.ParseInt(val, 10, 64); err == nil {
			if robj.CheckEncoding(obj.EncodingRaw) {
				robj.SetVal(numval)
				robj.SetEncoding(obj.EncodingInt)
			}
		}
	}

	return nil
}

func Int64Val(robj *obj.Robj) (int64, bool) {
	if !robj.CheckType(obj.TypeString) {
		return 0, false
	}
	if robj.CheckEncoding(obj.EncodingRaw) {
		TryObjectEncoding(robj)
	}
	if robj.CheckEncoding(obj.EncodingInt) {
		return unwrapInt(robj), true
	}
	return 0, false
}

// unwrap unwrap robj to obtain SDS. before unwrapping, the encoding type should be checked first.
// Unsafe
func unwrap(robj *obj.Robj) *SDS {
	sds := robj.Val().(SDS)
	return &sds
}

// unwrap unwrap robj to obtain int64. before unwrapping, the encoding type should be checked first.
// Unsafe
func unwrapInt(robj *obj.Robj) int64 {
	return robj.Val().(int64)
}
