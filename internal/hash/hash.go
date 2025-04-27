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

func HashGet(robj *obj.Robj, key []byte) []byte {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		return robj.Val().(*zipmap).get(key)
	}
	return nil
}

func HashDel(robj *obj.Robj, key []byte) {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		robj.Val().(*zipmap).del(key)
	}
	return
}

func Exists(robj *obj.Robj, key []byte) bool {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		idx, _ := robj.Val().(*zipmap).find(key)
		return idx > 0
	}
	return false
}
