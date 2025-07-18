package sds

import (
	"slices"
)

type SDS []byte

func New(bytes []byte) SDS {
	sds := (SDS)(bytes)
	return sds
}

func NewEmpty() SDS {
	var bytes = make([]byte, 0, 0)
	return New(bytes)
}

func (s SDS) deepcopy() SDS {
	b := make([]byte, 0, len(s))
	copy(b, s)
	return New(b)
}

func (s SDS) Len() int {
	return len(s)
}

func (s SDS) IsEmpty() bool {
	return len(s) == 0
}

func (s SDS) Cap() int {
	return cap(s)
}

func (s SDS) String() string {
	return string(s)
}

func (s SDS) Dup() SDS {
	bytes := make([]byte, len(s), cap(s))
	copy(bytes, s)
	return New(bytes)
}

func (s SDS) DupLine() SDS {
	newline, ok := s.SplitNewLine()
	if !ok {
		return NewEmpty()
	}
	return New(newline)
}

func (s SDS) Empty() {
	if s.Len() == 0 {
		return
	}
	s = s[:0]
}

func (s SDS) Cat(b []byte) {
	s = append(s, b...)
}

func (s SDS) Cmp(t SDS) int {
	return slices.Compare(s, t)
}

func Join(strs []string, sep string) SDS {
	sdss := make([]SDS, 0, len(strs))
	for i := 0; i < len(strs); i++ {
		sdss = append(sdss, New([]byte(strs[i])))
	}
	return JoinSDS(sdss, sep)
}

func JoinSDS(sdss []SDS, sep string) SDS {
	s := NewEmpty()
	if len(sdss) == 0 {
		return s
	}

	seps := New([]byte(sep))
	for i := 0; i < len(sdss); i++ {
		s.Cat(sdss[i])
		if i != len(sdss)-1 {
			s.Cat(seps)
		}
	}
	return s
}

func (s SDS) Cpy(t string) {
	if s.Len() > len(t) {
		s = s[:len(t)]
	}
	copy(s, t[:len(s)])
	if len(s) < len(t) {
		s = append(s, (t[len(s):])...)
	}
}

func (s SDS) Equal(t SDS) bool {
	return slices.Equal(s, t)
}

func (s SDS) SplitNewLine() ([]byte, bool) {
	idx := slices.Index(s, '\n')
	if idx == -1 {
		return nil, false
	}
	if s[idx-1] == '\r' {
		idx -= 1
	}
	newline := s[:idx]
	// skip '\r\n'
	s = s[idx+2:]
	return newline, true
}

func (s SDS) FirstByte() byte {
	return s[0]
}

func (s SDS) SetAt(offset int, newVal []byte) SDS {
	s = s.growZero(offset + len(newVal))
	help := s[offset:]
	copy(help, newVal)
	return s
}

func (s SDS) growZero(newLen int) SDS {
	curLen := s.Len()
	if curLen >= newLen {
		return s
	}
	growLen := newLen - curLen
	grows := make([]byte, growLen, growLen)
	s = append(s, grows...)
	return s
}

func (s SDS) Range(start, end int) []byte {
	if start < 0 {
		start = len(s) + start
	}
	if end < 0 {
		end = len(s) + end
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= len(s) {
		end = len(s) - 1
	}
	if start > end || len(s) == 0 {
		return []byte{}
	}
	return s[start : end+1]
}
