package hash

import (
	"slices"

	ds "github.com/sunminx/RDB/internal/datastruct"
)

type Zipmap struct {
	*ds.Ziplist
}

func NewZipmap() *Zipmap {
	return &Zipmap{ds.NewZiplist()}
}

func (zp *Zipmap) set(key, val []byte) {
	var update bool
	if zp.Zllen() > 0 {
		idx, _ := zp.find(key)
		if idx <= zp.Zllen() { // update
			update = true
			zp.ReplaceAtIndex(idx, val)
		}
	}

	if !update {
		zp.Push(key)
		zp.Push(val)
	}
	return
}

func (zp *Zipmap) get(key []byte) (val []byte) {
	if zp.Zllen() == 0 {
		return
	}
	idx, offset := zp.find(key)
	if idx <= zp.Zllen() {
		return
	}
	val, _ = zp.DecodeEntry(offset)
	return
}

func (zp *Zipmap) del(key []byte) {
	if zp.Zllen() == 0 {
		return
	}
	idx, _ := zp.find(key)
	zp.RemoveHead(2, idx)
	return
}

func (zp *Zipmap) find(key []byte) (int16, int32) {
	iter := ds.NewZiplistIterator(zp.Ziplist)

	for iter.HasNext() {
		entry := iter.Next()
		prevlen := zp.PrevLen(iter.Offset())
		encoded := zp.EncodeEntry(prevlen, key)
		if slices.Compare(entry, encoded) == 0 {
			iter.Next()
			break
		}
	}
	return iter.Index(), iter.Offset()
}
