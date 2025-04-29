package hash

import (
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

type hash interface {
	Set(*obj.Robj, []byte, []byte)
	Get(*obj.Robj, []byte) ([]byte, bool)
	Del(*obj.Robj, []byte) bool
	Exists(*obj.Robj, []byte) bool
}

func NewRobj(val any) *obj.Robj {
	robj := obj.NewRobj(val)
	robj.SetType(obj.ObjHash)
	robj.SetEncoding(obj.ObjEncodingZipmap)
	return robj
}

func Set(robj *obj.Robj, field, val sds.SDS) {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		unwrap(robj).set(field.Bytes(), val.Bytes())
	}
	return
}

func Get(robj *obj.Robj, field []byte) ([]byte, bool) {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		return unwrap(robj).get(field), true
	}
	return nil, false
}

func Del(robj *obj.Robj, field []byte) {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		unwrap(robj).del(field)
	}
	return
}

func Exists(robj *obj.Robj, field []byte) bool {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		idx, _ := unwrap(robj).find(field)
		return idx > 0
	}
	return false
}

func unwrap(robj *obj.Robj) *Zipmap {
	return robj.Val().(*Zipmap)
}
