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
	return obj.New(val, obj.TypeHash, obj.EncodingZipmap)
}

func DeepCopy(robj *obj.Robj) *obj.Robj {
	if robj.CheckEncoding(obj.EncodingZipmap) {
		nzm := unwrap(robj).deepcopy()
		return NewRobj(nzm)
	}
	return nil
}

func Set(robj *obj.Robj, field, val []byte) {
	if robj.CheckEncoding(obj.EncodingZipmap) {
		unwrap(robj).set(field, val)
	}
	return
}

func Get(robj *obj.Robj, field []byte) ([]byte, bool) {
	if robj.CheckEncoding(obj.EncodingZipmap) {
		return unwrap(robj).get(field), true
	}
	return nil, false
}

func Del(robj *obj.Robj, field []byte) {
	if robj.CheckEncoding(obj.EncodingZipmap) {
		unwrap(robj).del(field)
	}
	return
}

func Len(robj *obj.Robj) int64 {
	if robj.CheckEncoding(obj.EncodingZipmap) {
		return int64(unwrap(robj).hlen())
	}
	return 0
}

func Exists(robj *obj.Robj, field []byte) bool {
	if robj.CheckEncoding(obj.EncodingZipmap) {
		return unwrap(robj).exists(field)
	}
	return false
}

func unwrap(robj *obj.Robj) *Zipmap {
	return robj.Val().(*Zipmap)
}

type KVPair [2][]byte

func NewIterator(robj *obj.Robj) obj.Iterator {
	if robj.CheckEncoding(obj.EncodingZipmap) {
		zm := robj.Val().(*Zipmap)
		return newZipmapIterator(zm)
	}
	return nil
}
