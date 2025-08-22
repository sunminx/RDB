package hash

import (
	"slices"

	ds "github.com/sunminx/RDB/internal/datastruct"
)

type Zipmap struct {
	zl *ds.Ziplist
}

func NewZipmap() *Zipmap {
	return &Zipmap{zl: ds.NewZiplist()}
}

func (zm *Zipmap) SetZl(zl *ds.Ziplist) { zm.zl = zl }
func (zm *Zipmap) Zl() *ds.Ziplist      { return zm.zl }
func (zm *Zipmap) Len() uint16          { return zm.zl.Len() }
func (zm *Zipmap) Bytes() uint32        { return zm.zl.Bytes() }

func (zp *Zipmap) deepcopy() *Zipmap {
	nzl := zp.zl.DeepCopy()
	return &Zipmap{nzl}
}

func (zp *Zipmap) set(field, val []byte) {
	var update bool
	n := zp.zl.Len()
	if n > 0 {
		idx, _, _ := zp.find(field)
		if idx < n { // update
			update = true
			zp.zl.ReplaceAtIndex(idx, val)
		}
	}

	if !update {
		zp.zl.Push(field)
		zp.zl.Push(val)
	}
}

func (zp *Zipmap) get(field []byte) (val []byte) {
	n := zp.zl.Len()
	if n == 0 {
		return
	}
	idx, offset, _ := zp.find(field)
	if idx >= n {
		return
	}
	val, _ = zp.zl.DecodeEntry(offset)
	return val
}

func (zp *Zipmap) del(field []byte) {
	if zp.zl.Len() == 0 {
		return
	}
	idx, _, _ := zp.find(field)
	zp.zl.RemoveHead(2, idx-1)
	return
}

func (zp *Zipmap) exists(field []byte) bool {
	n := zp.zl.Len()
	if n == 0 {
		return false
	}
	_, _, ok := zp.find(field)
	return ok
}

func (zp *Zipmap) HLen() uint16 {
	return zp.zl.Len() / 2
}

func (zp *Zipmap) find(field []byte) (uint16, uint32, bool) {
	for entry := range zp.zl.Iter() {
		if slices.Compare(entry.Content, field) == 0 {
			return entry.Index, entry.Offset, true
		}
	}
	return 0, 0, false
}

type Pair [][]byte

func (zp *Zipmap) Iter() <-chan Pair {
	ch := make(chan Pair)
	go func() {
		defer close(ch)
		var pair [][]byte
		idx := 0
		for entry := range zp.zl.Iter() {
			if idx%2 == 0 {
				pair[0] = entry.Content
			}
			if idx%2 == 1 {
				pair[1] = entry.Content
				ch <- Pair(pair)
				pair = [][]byte{}
			}
			idx++
		}
	}()
	return ch
}
