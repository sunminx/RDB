package sds

import obj "github.com/sunminx/RDB/internal/object"

type sds interface {
	Append(*obj.Robj, sds.SDS)
	Len(*obj.Robj) int64
	Incr(*obj.Robj, int64)
}

func Append(robj *obj.Robj, s sds.SDS) {
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

func Incr(robj *obj.Robj, n int64) *obj.Robj {
	if robj.CheckEncoding(obj.ObjEncodingInt) {
		return obj.NewRobj(unwrapInt(robj) + n)
	}
	return
}

func digit10(n int64) int64 {
	var _len int64
	if n < 0 {
		_len = 1
		n = -n
	}

	if n < 10 {
		return _len + 1
	}
	if n < 100 {
		return _len + 2
	}
	if n < 1000 {
		return _len + 3
	}
	// < 12
	if n < 1000000000000 {
		// 4-8
		if n < 100000000 {
			// 4-6
			if n < 1000000 {
				// 4 [1000, 9999]
				if n < 10000 {
					return _len + 4
				}
				// 5-6
				return _len + 5 + cond(n >= 100000)
			}
			// 7-8
			return _len + 7 + cond(n >= 10000000)
		}
		if n < 10000000000 {
			return _len + 9 + cond(n >= 1000000000)
		}
		return _len + 11 + cond(n >= 100000000000)
	}
	return _len + 12 + digit10(n/1000000000000)
}

func unwrap(robj *obj.Robj) sds.SDS {
	return robj.Val().(sds.SDS)
}

func unwrapInt(robj *obj.Robj) sds.SDS {
	return robj.Val().(int64)
}
