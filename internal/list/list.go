package list

import obj "github.com/sunminx/RDB/internal/object"

// list is merely a declaration reflecting which interfaces are provided.
// do not attempt to reference it.
type list interface {
	Push(*obj.Robj, []byte)
	PushLeft(*obj.Robj, []byte)
	Set(*obj.Robj, int64, []byte)
	Pop(*obj.Robj) []byte
	PopLeft(*obj.Robj) []byte
	Index(*obj.Robj, int64) ([]byte, bool)
	Len(*obj.Robj) int64
	Range(*obj.Robj, int64, int64) [][]byte
	Trim(*obj.Robj, int64, int64)
}

func NewRobj(val any) *obj.Robj {
	robj := obj.NewRobj(val)
	robj.SetType(obj.ObjList)
	robj.SetEncoding(obj.ObjEncodingQuicklist)
	return robj
}

func Push(robj *obj.Robj, entry []byte) {
	if robj.CheckEncoding(obj.ObjEncodingQuicklist) {
		unwrap(robj).Push(entry)
	}
	return
}

func PushLeft(robj *obj.Robj, entry []byte) {
	if robj.CheckEncoding(obj.ObjEncodingQuicklist) {
		unwrap(robj).PushLeft(entry)
	}
	return
}

func Set(robj *obj.Robj, idx int64, entry []byte) {
	if robj.CheckEncoding(obj.ObjEncodingQuicklist) {
		unwrap(robj).ReplaceAtIndex(idx, entry)
	}
	return
}

func Pop(robj *obj.Robj) {
	if robj.CheckEncoding(obj.ObjEncodingQuicklist) {
		unwrap(robj).Pop()
	}
	return
}

func PopLeft(robj *obj.Robj) {
	if robj.CheckEncoding(obj.ObjEncodingQuicklist) {
		unwrap(robj).PopLeft()
	}
	return
}

func Index(robj *obj.Robj, idx int64) ([]byte, bool) {
	if robj.CheckEncoding(obj.ObjEncodingQuicklist) {
		return unwrap(robj).Index(idx)
	}
	return nil, false
}

func Len(robj *obj.Robj) int64 {
	if robj.CheckEncoding(obj.ObjEncodingQuicklist) {
		return unwrap(robj).Len()
	}
	return 0
}

func Range(robj *obj.Robj, start, end int64) [][]byte {
	if robj.CheckEncoding(obj.ObjEncodingQuicklist) {
		return unwrap(robj).Range(start, end)
	}
	return nil
}

func Trim(robj *obj.Robj, start, end int64) {
	if robj.CheckEncoding(obj.ObjEncodingQuicklist) {
		unwrap(robj).Trim(start, end)
	}
	return
}

// unwrap unwrap robj to obtain Quicklist. before unwrapping, the encoding type should be checked first.
// Unsafe
func unwrap(robj *obj.Robj) *Quicklist {
	return robj.Val().(*Quicklist)
}
