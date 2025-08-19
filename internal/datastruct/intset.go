package datastruct

import (
	"math"
)

const (
	IntSetEncInt16   = 1 << (iota + 1)
	IntSetEncInt32   = 1 << (iota + 1)
	IntSetEncInt64   = 1 << (iota + 1)
	IntSetEncUnknown = 0
)

// IntSet stores integer values in order using contiguous memory space.
type IntSet struct {
	encoding int
	length   int
	content  []byte
}

func NewIntSet() *IntSet {
	return &IntSet{
		encoding: IntSetEncUnknown,
		length:   0,
		content:  nil,
	}
}

func (s *IntSet) Encoding() int { return s.encoding }
func (s *IntSet) Length() int   { return s.length }

// Add inserts an integer value int the intset.
func (s *IntSet) Add(val int64) bool {
	valenc := intsetValEncoding(val)
	if valenc > s.encoding {
		s.upgrade(valenc)
	}
	pos := 0
	for i := 0; i < s.length; i++ {
		nval := s.get(pos)
		if nval == val {
			return true
		} else if val < nval {
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
	if enc > s.encoding {
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

func (s *IntSet) upgrade(newenc int) {
	content := make([]byte, newenc*(s.length+1)*2)
	dst := content
	for i := 0; i < s.length; i++ {
		copy(dst, s.content[i*s.encoding:(i+1)*s.encoding])
		dst = dst[newenc:]
	}
	s.encoding = newenc
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
		return mid*s.encoding, true
	}
	return lo*s.encoding, false
}

func (s *IntSet) set(pos int, val int64) {
	// Scale up the capacity of contiguous space if needed.
	if s.encoding * (s.length+1) >= cap(s.content) {
		content := make([]byte, s.encoding*(s.length+1))
		copy(content, s.content)
		s.content = content
	}
	// Move all elemtent after the position backward by the encoding distance.
	if pos < s.encoding * s.length { 
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
	v := int64(0)
	b := s.content[pos : pos+s.encoding]
	// Get the exact encoded length of the element.
	// Beause of the position of the symbol bit we have to know.
	enc := intsetEleEncoding(b)
	if enc == IntSetEncInt16 {
		v = int64(b[0]) + int64(int8(b[1]))<<8
	} else if enc == IntSetEncInt32 {
		v = int64(b[0]) + int64(b[1])<<8
		v += int64(b[2])<<16 + int64(int8(b[3]))<<24
	} else if enc == IntSetEncInt64 {
		v = int64(b[0]) + int64(b[1])<<8
		v += int64(b[2])<<16 + int64(b[3])<<24
		v += int64(b[4])<<32 + int64(b[5])<<40
		v += int64(b[6])<<48 + int64(int8(b[7]))<<56
	}
	return v
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

func intsetEleEncoding(b []byte) int {
	if len(b) == IntSetEncInt64 {
		if b[7] == 0 && b[6] == 0 &&
			b[5] == 0 && b[4] == 0 {
			if b[3] == 0 && b[2] == 0 {
				return IntSetEncInt16
			}
			return IntSetEncInt32
		}
		return IntSetEncInt64
	}
	if len(b) == IntSetEncInt32 {
		if b[3] == 0 && b[2] == 0 {
			return IntSetEncInt16
		}
		return IntSetEncInt32	
	}
	return IntSetEncInt16
}
