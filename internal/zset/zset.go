package zset

import robj "github.com/sunminx/RDB/internal/object"

type ZSet struct {
	ln   uint64
	impl zsetter
}

func New() *robj.Robj {
	zs := new(ZSet)
	return robj.New(zs, robj.TypeZSet, robj.EncodingZiplist)
}

type implType int

const (
	ziplistImplType = iota + 1
	zsetImplType
)

type zsetter interface {
	add(float64, []byte) uint64
	incr(float64, []byte) uint64
	remove([]byte) uint64
	removeRangeByRank(uint64, uint64, bool, bool) uint64
	removeRangeByEle([]byte, []byte, bool, bool) uint64
	removeRangeByScore(float64, float64, bool, bool) uint64
	rangeByRank(uint64, uint64, bool, bool, int) [][]byte
	rangeByEle([]byte, []byte, bool, bool, int) [][]byte
	rangeByScore(float64, float64, bool, bool, int) [][]byte
	countByRank(uint64, uint64, bool, bool, int) uint64
	countByEle([]byte, []byte, bool, bool, int) uint64
	countByScore(float64, float64, bool, bool, int) uint64
	rank([]byte, int) uint64
	score([]byte) float64
	typ() implType
}
