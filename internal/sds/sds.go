package sds

import (
	"strconv"

	"github.com/sunminx/RDB/internal/object"
	obj "github.com/sunminx/RDB/internal/object"
)

type sds interface {
	Append(*obj.Robj, []byte)
	Len(*obj.Robj) int64
	Incr(*obj.Robj, int64) int64
}

func NewRobj(val any) *obj.Robj {
	robj := obj.Robj{}
	robj.SetType(obj.ObjString)
	switch val.(type) {
	case SDS:
		robj.SetEncoding(obj.ObjEncodingRaw)
	case []byte:
		val = New(val.([]byte))
		robj.SetEncoding(obj.ObjEncodingRaw)
	case int64:
		robj.SetEncoding(obj.ObjEncodingInt)
	}
	robj.SetVal(val)
	return &robj
}

func Append(robj *obj.Robj, s []byte) {
	if robj.CheckEncoding(obj.ObjEncodingRaw) {
		unwrap(robj).Cat(s)
	}
	return
}

func Len(robj *obj.Robj) int64 {
	if robj.CheckEncoding(obj.ObjEncodingRaw) {
		return int64(unwrap(robj).Len())
	}
	if robj.CheckEncoding(obj.ObjEncodingInt) {
		return digit10(unwrapInt(robj))
	}
	return 0
}

func Incr(robj *obj.Robj, n int64) int64 {
	_ = TryObjectEncoding(robj)
	if robj.CheckEncoding(obj.ObjEncodingInt) {
		n += unwrapInt(robj)
		robj.SetVal(n)
		return n
	}
	return 0
}

func digit10(n int64) int64 {
	var ln int64
	if n < 0 {
		ln = 1
		n = -n
	}

	if n < 10 {
		return ln + 1
	}
	if n < 100 {
		return ln + 2
	}
	if n < 1000 {
		return ln + 3
	}
	// < 12
	if n < 1000000000000 {
		// 4-8
		if n < 100000000 {
			// 4-6
			if n < 1000000 {
				// 4 [1000, 9999]
				if n < 10000 {
					return ln + 4
				}
				// 5-6
				return ln + 5 + cond(n >= 100000)
			}
			// 7-8
			return ln + 7 + cond(n >= 10000000)
		}
		if n < 10000000000 {
			return ln + 9 + cond(n >= 1000000000)
		}
		return ln + 11 + cond(n >= 100000000000)
	}
	return ln + 12 + digit10(n/1000000000000)
}

func cond(c bool) int64 {
	if c {
		return 1
	}
	return 0
}

func TryObjectEncoding(robj *obj.Robj) error {
	if !robj.CheckType(object.ObjString) {
		return nil
	}
	if !robj.SDSEncodedObject() {
		return nil
	}

	var numval int64
	var err error
	val := unwrap(robj).String()
	if len(val) <= 20 {
		if numval, err = strconv.ParseInt(val, 10, 64); err == nil {
			if robj.CheckEncoding(obj.ObjEncodingRaw) {
				robj.SetVal(numval)
				robj.SetEncoding(obj.ObjEncodingInt)
			}
		}
	}

	return nil
}

func Int64Val(robj *obj.Robj) (int64, bool) {
	if !robj.CheckType(obj.ObjString) {
		return 0, false
	}
	if robj.SDSEncodedObject() {
		TryObjectEncoding(robj)
	}
	if robj.CheckEncoding(obj.ObjEncodingInt) {
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
