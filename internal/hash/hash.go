package hash

import (
	obj "github.com/sunminx/RDB/internal/object"
)

type hash interface {
	Set(*obj.Robj, []byte, []byte)
	Get(*obj.Robj, []byte) ([]byte, bool)
	Del(*obj.Robj, []byte) bool
	Len(*obj.Robj) int64
	Exists(*obj.Robj, []byte) bool
}

func NewRobj(val any) *obj.Robj {
	robj := obj.NewRobj(val)
	robj.SetType(obj.ObjHash)
	robj.SetEncoding(obj.ObjEncodingZipmap)
	return robj
}

func Set(robj *obj.Robj, field, val []byte) {
	if robj.CheckEncoding(obj.ObjEncodingZipmap) {
		unwrap(robj).set(field, val)
	}
	return
}

func Get(robj *obj.Robj, field []byte) ([]byte, bool) {
	if robj.CheckEncoding(obj.ObjEncodingZipmap) {
		return unwrap(robj).get(field), true
	}
	return nil, false
}

func Del(robj *obj.Robj, field []byte) {
	if robj.CheckEncoding(obj.ObjEncodingZipmap) {
		unwrap(robj).del(field)
	}
	return
}

func Len(robj *obj.Robj) int64 {
	if robj.CheckEncoding(obj.ObjEncodingZipmap) {
		return int64(unwrap(robj).hlen())
	}
	return 0
}

func Exists(robj *obj.Robj, field []byte) bool {
	if robj.CheckEncoding(obj.ObjEncodingZipmap) {
		return unwrap(robj).exists(field)
	}
	return false
}

func unwrap(robj *obj.Robj) *Zipmap {
	return robj.Val().(*Zipmap)
}
