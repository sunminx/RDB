package datastruct

import (
	"math"
)

const (
	IntSetEncInt16 = 1 << (iota + 1)
	IntSetEncInt32 = 1 << (iota + 1)
	IntSetEncInt64 = 1 << (iota + 1)
)

// IntSet stores integer values in order using contiguous memory space.
type IntSet struct {
	encoding int
	length   int
	content  []byte
}

func NewIntSet() *IntSet {
	return &IntSet{
		encoding: IntSetEncInt16,
		length:   0,
		content:  make([]byte, 0, 1024),
	}
}

func (s *IntSet) Encoding() int { return s.encoding }
func (s *IntSet) Length() int   { return s.length }

// Add inserts an integer value int the intset.
func (s *IntSet) Add(val int64) bool {
	valenc := intsetValEncoding(val)
	//if valenc > s.encoding {
	s.upgrade(valenc)
	//}
	pos := 0
	for i := 0; i < s.length; i++ {
		if val < s.get(pos) {
			break
		}
		pos += s.encoding
	}
	s.set(pos, val)
	s.length++
	return true
}

// Remove deletes the integer val from intset.
func (s *IntSet) Remove(val int64) bool {
	enc := intsetValEncoding(val)
	if enc != s.encoding {
		return false
	}
	pos, ok := s.search(val)
	if !ok {
		return false
	}
	copy(s.content[pos:], s.content[pos+s.encoding:])
	s.length--
	return true
}

// Get returns the value at pos.
func (s *IntSet) Get(pos int) (int64, bool) {
	if pos < 0 || pos >= s.length*s.encoding ||
		pos%s.encoding != 0 {
		return 0, false
	}
	val := s.get(pos)
	return val, true
}

// Find indicates the value if in intset.
func (s *IntSet) Find(val int64) bool {
	enc := intsetValEncoding(val)
	if enc > s.encoding {
		return false
	}
	_, ok := s.search(val)
	return ok
}

func (s *IntSet) upgrade(newEnc int) {
	content := make([]byte, newEnc*(s.length+1))
	dst := content
	for i := 0; i < s.length; i++ {
		copy(dst, s.content[i*s.encoding:(i+1)*s.encoding])
		dst = dst[(i+1)*newEnc:]
	}
	s.encoding = newEnc
	s.content = content
}

func (s *IntSet) search(val int64) (int, bool) {
	if s.length == 0 {
		return 0, false
	}
	lo, hi := 0, s.length-1
	if val < s.get(lo) || val > s.get(hi*s.encoding) {
		return 0, false
	}
	cur, mid := int64(-1), -1
	for lo <= hi {
		mid = lo + (hi-lo)/2
		cur = s.get(mid * s.encoding)
		if val < cur {
			hi = mid - 1
		} else if val > cur {
			lo = mid + 1
		} else {
			break
		}
	}
	if val == cur {
		return mid, true
	}
	return lo, false
}

func (s *IntSet) set(pos int, val int64) {
	if pos < s.length*s.encoding {
		copy(s.content[pos+s.encoding:], s.content[pos:])
	}
	b := s.content[pos : pos+s.encoding]
	v := uint64(val)
	if s.encoding >= IntSetEncInt16 {
		b[0] = byte(v)
		b[1] = byte(v >> 8)
	}
	if s.encoding >= IntSetEncInt32 {
		b[2] = byte(v >> 16)
		b[3] = byte(v >> 24)
	}
	if s.encoding >= IntSetEncInt64 {
		b[4] = byte(v >> 32)
		b[5] = byte(v >> 40)
		b[6] = byte(v >> 48)
		b[7] = byte(v >> 56)
	}
}

func (s *IntSet) get(pos int) int64 {
	v := uint64(0)
	b := s.content[pos : pos+s.encoding]
	if s.encoding >= IntSetEncInt16 {
		v = uint64(b[0]) + uint64(b[1])<<8
	}
	if s.encoding >= IntSetEncInt32 {
		v |= uint64(b[2])<<16 + uint64(b[3])<<24

	}
	if s.encoding >= IntSetEncInt64 {
		v |= uint64(b[4])<<32 + uint64(b[5])<<40
		v |= uint64(b[6])<<48 + uint64(b[7])<<56
	}
	return int64(v)
}

func intsetValEncoding(val int64) int {
	if val < math.MinInt32 || val > math.MaxInt32 {
		return IntSetEncInt64
	}
	if val < math.MinInt16 || val > math.MaxInt16 {
		return IntSetEncInt32
	}
	return IntSetEncInt16
}
