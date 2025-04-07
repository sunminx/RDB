package sds

import "slices"

type SDS []byte

func New(ptr []byte) SDS {
	return (SDS)(ptr)
}

func NewEmpty() SDS {
	var bytes []byte
	return (SDS)(bytes)
}

func (s *SDS) Len() int {
	return len(([]byte)(*s))
}

func (s *SDS) Cap() int {
	return cap(([]byte)(*s))
}

func (s *SDS) Bytes() []byte {
	return ([]byte)(*s)
}

func (s *SDS) Dup() SDS {
	bytes := make([]byte, s.Len(), s.Cap())
	copy(bytes, ([]byte)(*s))
	return New(bytes)
}

func (s *SDS) Empty() {
	if s.Len() == 0 {
		return
	}
	(*s) = (*s)[:0]
}

func (s *SDS) Cat(t *SDS) {
	(*s) = append(*s, (*t)...)
}

func (s *SDS) Cmp(t *SDS) int {
	return slices.Compare(([]byte)(*s), ([]byte)(*t))
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
		s.Cat(&sdss[i])
		if i != len(sdss)-1 {
			s.Cat(&seps)
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
