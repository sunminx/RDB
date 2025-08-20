package set

import (
	"strconv"

	robj "github.com/sunminx/RDB/internal/object"
)

// Set is a collection type that does not contain duplicate element.
// When the number of elements is less than 'setMaxIntsetEntries', we use intset
// as the underlying implementation, otherwise, use dict.
type Set struct {
	size int64
	impl seter
}

type seter interface {
	add([]byte) bool
	remove([]byte) bool
	isMember([]byte) bool
	iter() <-chan []byte
	typ() implType
}

func New() *robj.Robj {
	s := new(set)
	s.impl = newIntsetImpl()
	return robj.New(s, robj.TypeSet, robj.EncodingIntset)
}

func (s *Set) Add(ele []byte) bool {
	n, ok := strconv.ParseInt(string(ele), 10, 64)
	s.convertImplTypeIfNeeded(ok)
	ok = s.impl.add(ele)
	if ok {
		s.size++
	}
	return ok
}

func (s *Set) Remove(ele []byte) bool {
	return s.impl.remove(ele)
}

func (s *Set) IsMember(ele []byte) bool {
	return s.impl.isMember(ele)
}

func (s *Set) Size() uint64 {
	return s.size
}

func (s *Set) Iter() <-chan []byte {
	return s.impl.iter()
}

type implType int

const (
	intsetImplType implType = iota + 1
	dictImplType
)

const setMaxIntsetEntries = 512

func (s *Set) convertImplTypeIfNeeded(isNum bool) {
	if s.impl.typ() == intsetImplType &&
		(!isNum || s.ln >= setMaxIntsetEntries) {
		impl := newDictImpl()
		for ele := range s.impl.iter() {
			impl.add(ele)
		}
		s.impl = impl
	} else if s.impl.typ() == dictImplType &&
		s.size == 0 && isNum {
		s.impl = newIntsetImpl()
	}
	return
}
