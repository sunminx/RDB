package list

import obj "github.com/sunminx/RDB/internal/object"

type hash interface {
	Set([]byte, []byte)
	Get([]byte, []byte)
	Del([]byte) bool
	Exists([]byte) bool
}

func HashSet(robj *obj.Robj, key, val []byte) {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		robj.Val().(*zipmap).set(key, val)
	}
	return
}

func HashGet(obj *robj.Robj, key, val []byte) {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		return robj.Val().(*zipmap).get(key)
	}
	return
}

func HashDel(obj *robj.Robj, key []byte) {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		robj.Val().(*zipmap).del(key)
	}
	return
}

func Exists(obj *robj.Robj, key []byte) {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		robj.Val().(*zipmap).exists(key)
	}
	return
}
