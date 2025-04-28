package hash

import (
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

type hash interface {
	Set(*obj.Robj, sds.SDS, sds.SDS)
	Get(*obj.Robj, sds.SDS) ([]byte, bool)
	Del(*obj.Robj, sds.SDS) bool
	Exists(*obj.Robj, sds.SDS) bool
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

func Get(robj *obj.Robj, field sds.SDS) ([]byte, bool) {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		return unwrap(robj).get(field.Bytes()), true
	}
	return nil, false
}

func Del(robj *obj.Robj, field sds.SDS) {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		unwrap(robj).del(field.Bytes())
	}
	return
}

func Exists(robj *obj.Robj, field sds.SDS) bool {
	if robj.CheckEncoding(obj.ObjEncodingZiplist) {
		idx, _ := unwrap(robj).find(field.Bytes())
		return idx > 0
	}
	return false
}

func unwrap(robj *obj.Robj) *Zipmap {
	return robj.Val().(*Zipmap)
}
