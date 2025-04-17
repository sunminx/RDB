package sds

import (
	"slices"
)

type SDS []byte

func New(bytes []byte) SDS {
	return (SDS)(bytes)
}

func NewEmpty() SDS {
	var bytes = make([]byte, 0, 0)
	return (SDS)(bytes)
}

func (s *SDS) Len() int {
	return len(([]byte)(*s))
}

func (s *SDS) IsEmpty() bool {
	return s.Len() == 0
}

func (s *SDS) Cap() int {
	return cap(([]byte)(*s))
}

func (s *SDS) Bytes() []byte {
	return ([]byte)(*s)
}

func (s *SDS) String() string {
	return string(s.Bytes())
}

func (s *SDS) Dup() SDS {
	bytes := make([]byte, s.Len(), s.Cap())
	copy(bytes, ([]byte)(*s))
	return New(bytes)
}

func (s *SDS) DupLine() SDS {
	newline, ok := s.SplitNewLine()
	if !ok {
		return NewEmpty()
	}
	return New(newline)
}

func (s *SDS) Empty() {
	if s.Len() == 0 {
		return
	}
	(*s) = (*s)[:0]
}

func (s *SDS) Cat(t SDS) {
	(*s) = append(*s, t...)
}

func (s *SDS) Cmp(t SDS) int {
	return slices.Compare(([]byte)(*s), ([]byte)(t))
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

func (s *SDS) Cpy(t string) {
	if s.Len() > len(t) {
		(*s) = (*s)[:len(t)]
	}
	copy(([]byte)(*s), []byte(t[:s.Len()]))
	if s.Len() < len(t) {
		(*s) = append((*s), []byte(t[s.Len():])...)
	}
}

func (s *SDS) Equal(t SDS) bool {
	return slices.Equal(s.Bytes(), t.Bytes())
}

func (s *SDS) SplitNewLine() ([]byte, bool) {
	idx := slices.Index(s.Bytes(), '\n')
	if idx == -1 {
		return nil, false
	}
	if ([]byte)(*s)[idx-1] == '\r' {
		idx -= 1
	}
	newline := ([]byte)(*s)[:idx]
	// skip '\r\n'
	(*s) = (*s)[idx+2:]
	return newline, true
}

func (s *SDS) FirstByte() byte {
	return ([]byte)(*s)[0]
}
