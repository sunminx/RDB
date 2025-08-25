package zset

import (
	"strconv"
	"strings"
	"testing"
)

func TestZiplistImplAdd(t *testing.T) {
	impl := newZiplistImpl()
	for i := 0; i < 1024; i++ {
		if impl.add(float64(i), []byte(strings.Repeat(strconv.Itoa(i), 10))) != 1 {
			t.Error("add failed")
		}
	}
}

func TestZiplistImplRemoveRangeByRank(t *testing.T) {
	impl := newZiplistImpl()
	for i := 1; i < 1024; i++ {
		_ = impl.add(float64(i), []byte(strings.Repeat(strconv.Itoa(i), 10)))
	}
	n := impl.removeRangeByRank(0, 1022, true, true)
	t.Log(n)
	if n != 1022 {
		t.Error("removeRangeByRank failed")
	}
}

func TestZiplistImplRangeByRank(t *testing.T) {
	impl := newZiplistImpl()
	// TODO : the value is "0000000000"
	for i := 1; i < 1024; i++ {
		_ = impl.add(float64(i), []byte(strings.Repeat(strconv.Itoa(i), 10)))
	}
	eles := impl.rangeByRank(0, 1023, true, true, 0)
	for i := 0; i < 1023; i++ {
		if string(eles[i]) != strings.Repeat(strconv.Itoa(i+1), 10) {
			t.Error("rangeByRank failed")
		}
	}
}

func TestZiplistImplRangeByEle(t *testing.T) {
	impl := newZiplistImpl()
	for i := 1; i < 10; i++ {
		_ = impl.add(float64(i), []byte(strconv.Itoa(i)))
	}
	eles := impl.rangeByEle([]byte("1"), []byte("9"), true, true, 0)
	for i := 0; i < 9; i++ {
		if string(eles[i]) != strconv.Itoa(i+1) {
			t.Error("rangeByRank failed")
		}
	}
}

func TestZiplistImplRangeByScore(t *testing.T) {
	impl := newZiplistImpl()
	for i := 1; i < 1024; i++ {
		_ = impl.add(float64(i), []byte(strconv.Itoa(i)))
	}
	eles := impl.rangeByScore(float64(1), float64(1023), true, true, 0)
	for i := 0; i < 1023; i++ {
		if string(eles[i]) != strconv.Itoa(i+1) {
			t.Error("rangeByRank failed")
		}
	}
}

func TestZiplistImplScore(t *testing.T) {
	impl := newZiplistImpl()
	for i := 0; i < 1024; i++ {
		_ = impl.add(float64(i), []byte(strings.Repeat(strconv.Itoa(i), 10)))
	}
	for i := 0; i < 1024; i++ {
		if float64(i) != impl.score([]byte(strings.Repeat(strconv.Itoa(i), 10))) {
			t.Error("score failed")
		}
	}
}
