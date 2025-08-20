package set

import (
	"strconv"

	ds "github.com/sunminx/RDB/internal/datastruct"
)

type intsetImpl = ds.IntSet

func newIntsetImpl() seter {
	return intsetImpl(ds.NewIntSet())
}

func (impl *intsetImpl) add(ele []byte) bool {
	// TODO
	n, _ := strconv.ParseInt(string(ele), 10, 64)
	return impl.Add(n)
}

func (impl *intsetImpl) remove(ele []byte) bool {
	n, _ := strconv.ParseInt(string(ele), 10, 64)
	return impl.Remove(n)
}

func (impl *intsetImpl) isMember(ele []byte) bool {
	n, _ := strconv.ParseInt(string(ele), 10, 64)
	return impl.Find(n)
}

func (impl *intsetImpl) iter() <-chan []byte {
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		for i := 0; i < impl.Length(); i++ {
			n := impl.Get(i * impl.Encoding)
			// TODO
			ele := strconv.FormatInt(n, 64)
			ch <- []byte(ele)
		}
	}()
	return ch
}

func (impl *intsetImpl) typ() implType {
	return intsetImplType
}

type dictImpl = ds.Dict

func newDictImpl() seter {
	return dictImpl(ds.NewDict())
}

const empty = struct{}{}

func (impl dictImpl) add(ele []byte) bool {
	return impl.Put(string(ele), empty)
}

func (impl dictImpl) remove(ele []byte) bool {
	return impl.Remove(string(ele))
}

func (impl dictImpl) isMember(ele []byte) bool {
	_, ok := impl.Get(string(ele))
	return ok
}

func (impl dictImpl) iter() <-chan []byte {
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		for k := range impl {
			ch <- k
		}
	}()
	return ch
}

func (impl dictImpl) typ() implType {
	return dictImplType
}
