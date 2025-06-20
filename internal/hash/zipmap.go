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

func (zp *Zipmap) deepcopy() *Zipmap {
	zl := zp.Ziplist
	nzl := zl.DeepCopy()
	return &Zipmap{nzl}
}

func (zp *Zipmap) set(field, val []byte) {
	var update bool
	n := zp.Len()
	if n > 0 {
		idx, _ := zp.find(field)
		if idx < n { // update
			update = true
			zp.ReplaceAtIndex(idx, val)
		}
	}

	if !update {
		zp.Push(field)
		zp.Push(val)
	}
}

func (zp *Zipmap) get(field []byte) (val []byte) {
	n := zp.Len()
	if n == 0 {
		return
	}
	idx, offset := zp.find(field)
	if idx >= n {
		return
	}
	val, _ = zp.DecodeEntry(offset)
	return val
}

func (zp *Zipmap) del(field []byte) {
	if zp.Len() == 0 {
		return
	}
	idx, _ := zp.find(field)
	zp.RemoveHead(2, idx-1)
	return
}

func (zp *Zipmap) exists(field []byte) bool {
	n := zp.Len()
	if n == 0 {
		return false
	}
	idx, _ := zp.find(field)
	return idx < n
}

func (zp *Zipmap) HLen() uint16 {
	return zp.Len() / 2
}

func (zp *Zipmap) find(field []byte) (uint16, uint32) {
	iter := ds.NewZiplistIterator(zp.Ziplist)
	for iter.HasNext() {
		entry := iter.Next()
		if slices.Compare(entry, field) == 0 {
			break
		}
	}
	return iter.Index(), iter.Offset()
}

type ZipmapIterator struct {
	zlIter *ds.ZiplistIterator
}

func newZipmapIterator(zm *Zipmap) *ZipmapIterator {
	return &ZipmapIterator{ds.NewZiplistIterator(zm.Ziplist)}
}

func (iter *ZipmapIterator) HasNext() bool {
	return iter.zlIter.HasNext()
}

func (iter *ZipmapIterator) Next() any {
	return iter.next()
}

func (iter *ZipmapIterator) next() KVPair {
	k, v := iter.zlIter.Next(), iter.zlIter.Next()
	return KVPair([2][]byte{k, v})
}
