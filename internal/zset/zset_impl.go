package zset

import (
	"slices"

	ds "github.com/sunminx/RDB/internal/datastruct"
	"github.com/sunminx/RDB/pkg/util"
)

type ziplistImpl struct {
	*ds.Ziplist
}

func newZiplistImpl() zsetter {
	return &ziplistImpl{ds.NewZiplist()}
}

func (impl *ziplistImpl) add(score float64, ele []byte) uint64 {
	index, _, ok := impl.Find(ele)
	sb := util.EncodeFloat(score)
	if !ok {
		impl.Push(ele)
		impl.Push(sb)
		return 1
	}
	// Update score if element is exists.
	impl.ReplaceAtIndex(index+1, sb)
	return 0
}

func (impl *ziplistImpl) incr(incr float64, ele []byte) uint64 {
	index, _, ok := impl.Find(ele)
	if ok {
		// Ele and score always come in pairs, so the score must exists in here.
		entry, _ := impl.EntryAtIndex(index + 1)
		oldscore := util.DecodeFloat(entry)
		sb := util.EncodeFloat(oldscore + incr)
		impl.ReplaceAtIndex(index+1, sb)
		return 1
	}
	return 0
}

func (impl *ziplistImpl) remove(ele []byte) uint64 {
	_, offset, ok := impl.Find(ele)
	if !ok {
		return 0
	}
	_ = impl.RemoveFromPos(offset, 1)
	return 1
}

func (impl *ziplistImpl) removeRangeByRank(min, max uint64, minex, maxex bool) uint64 {
	_, n, _ := impl.RemoveHead(uint16(max-min), uint16(min))
	return uint64(n)
}

func (impl *ziplistImpl) removeRangeByEle(min, max []byte, minex, maxex bool) uint64 {
	minIdx, offset, ok := impl.Find(min)
	if !ok {
		return 0
	}
	maxIdx, _, ok := impl.Find(max)
	if !ok {
		return 0
	}
	removed := impl.RemoveFromPos(offset, 2*(maxIdx-minIdx))
	return uint64(len(removed))
}

func (impl *ziplistImpl) removeRangeByScore(min, max float64, minex, maxex bool) uint64 {
	minIdx, offset, ok := impl.Find(util.EncodeFloat(min))
	if !ok {
		return 0
	}
	maxIdx, _, ok := impl.Find(util.EncodeFloat(max))
	if !ok {
		return 0
	}
	// Get the offset of min element.
	offset = impl.PrevLen(offset)
	removed := impl.RemoveFromPos(offset, 2*(maxIdx-minIdx))
	return uint64(len(removed))
}

func (impl *ziplistImpl) rangeByRank(min, max uint64, minex, maxex bool, direct int) [][]byte {
	eles := make([][]byte, 0)
	min, max = 2*min, 2*max
	idx := uint64(0)
	for entry := range impl.Iter() {
		if idx%2 != 0 {
			idx++
			continue
		}
		if (maxex && idx > max) || (!maxex && idx >= max) {
			break
		}
		if (minex && idx >= min) || (!minex && idx > min) {
			eles = append(eles, entry.Content)
		}
		idx++
	}
	return eles
}

func (impl *ziplistImpl) rangeByEle(min, max []byte, minex, maxex bool, direct int) [][]byte {
	eles := make([][]byte, 0)
	idx := 0
	for entry := range impl.Iter() {
		if idx%2 != 0 {
			idx++
			continue
		}
		if (maxex && slices.Compare(entry.Content, max) > 0) ||
			(!maxex && slices.Compare(entry.Content, max) >= 0) {
			break
		}
		if (minex && slices.Compare(entry.Content, min) >= 0) ||
			(!minex && slices.Compare(entry.Content, min) > 0) {
			eles = append(eles, entry.Content)
		}
		idx++
	}
	return eles
}

func (impl *ziplistImpl) rangeByScore(min, max float64, minex, maxex bool, direct int) [][]byte {
	var (
		eles = make([][]byte, 0)
		idx  = 0
		ele  []byte
	)
	for entry := range impl.Iter() {
		if idx%2 == 0 {
			idx++
			ele = entry.Content
			continue
		}
		score := util.DecodeFloat(entry.Content)
		if (maxex && score > max) || (!maxex && score >= max) {
			break
		}
		if (minex && score >= min) || (!minex && score > min) {
			eles = append(eles, ele)
		}
		idx++
	}
	return eles
}

func (impl *ziplistImpl) countByRank(min, max uint64, minex, maxex bool, direct int) uint64 {
	eles := impl.rangeByRank(min, max, minex, maxex, direct)
	return uint64(len(eles))
}

func (impl *ziplistImpl) countByEle(min, max []byte, minex, maxex bool, direct int) uint64 {
	eles := impl.rangeByEle(min, max, minex, maxex, direct)
	return uint64(len(eles))
}

func (impl *ziplistImpl) countByScore(min, max float64, minex, maxex bool, direct int) uint64 {
	eles := impl.rangeByScore(min, max, minex, maxex, direct)
	return uint64(len(eles))
}

func (impl *ziplistImpl) rank(ele []byte, direct int) uint64 {
	idx := uint64(0)
	for entry := range impl.Iter() {
		if slices.Compare(ele, entry.Content) == 0 {
			break
		}
		idx++
	}
	return idx
}

func (impl *ziplistImpl) score(ele []byte) float64 {
	score := float64(0)
	finded := false
	for entry := range impl.Iter() {
		if finded {
			score = util.DecodeFloat(entry.Content)
			break
		}
		if slices.Compare(ele, entry.Content) == 0 {
			finded = true
		}
	}
	return score
}

func (impl *ziplistImpl) typ() implType {
	return ziplistImplType
}

type zsetImpl struct {
	dict ds.Dict
	zsl  *ds.Skiplist
}

func newZSetImpl() zsetter {
	return &zsetImpl{
		dict: ds.NewDict(),
		zsl:  ds.NewSkiplist(ds.BytesCompare),
	}
}

func (impl *zsetImpl) add(score float64, ele []byte) uint64 {
	v, ok := impl.dict.Get(string(ele))
	oldscore := v.(float64)
	if ok && score != oldscore {
		if impl.zsl.Update(oldscore, ele, score) != nil &&
			impl.dict.Put(string(ele), score) {
			return 1
		}
	}
	if !ok {
		if impl.zsl.Insert(score, ele) &&
			impl.dict.Put(string(ele), score) {
			return 1
		}
	}
	return 0
}

func (impl *zsetImpl) incr(incr float64, ele []byte) uint64 {
	v, ok := impl.dict.Get(string(ele))
	oldscore := v.(float64)
	if ok {
		score := oldscore + incr
		if impl.zsl.Update(oldscore, ele, score) != nil &&
			impl.dict.Put(string(ele), score) {
			return 1
		}
	}
	return 0
}

func (impl *zsetImpl) remove(ele []byte) uint64 {
	score, ok := impl.dict.Get(string(ele))
	if !ok {
		return 0
	}
	if impl.zsl.Delete(score.(float64), ele) &&
		impl.dict.Remove(string(ele)) {
		return 1
	}
	return 0
}

func (impl *zsetImpl) removeRangeByRank(min, max uint64, minex, maxex bool) uint64 {
	rs := ds.RankRange{min, max, minex, maxex}
	deleted := impl.zsl.DeleteRangeByRank(rs)
	for _, ele := range deleted {
		_ = impl.dict.Remove(string(ele.([]byte)))
	}
	return uint64(len(deleted))
}

func (impl *zsetImpl) removeRangeByEle(min, max []byte, minex, maxex bool) uint64 {
	rs := ds.EleRange{min, max, minex, maxex}
	deleted := impl.zsl.DeleteRangeByEle(rs)
	for _, ele := range deleted {
		_ = impl.dict.Remove(string(ele.([]byte)))
	}
	return uint64(len(deleted))
}

func (impl *zsetImpl) removeRangeByScore(min, max float64, minex, maxex bool) uint64 {
	rs := ds.ScoreRange{min, max, minex, maxex}
	deleted := impl.zsl.DeleteRangeByScore(rs)
	for _, ele := range deleted {
		_ = impl.dict.Remove(string(ele.([]byte)))
	}
	return uint64(len(deleted))
}

func (impl *zsetImpl) rangeByRank(min, max uint64, minex, maxex bool, direct int) [][]byte {
	rs := ds.RankRange{min, max, minex, maxex}
	eles := make([][]byte, 0)
	for _, ele := range impl.zsl.RangeByRank(rs) {
		eles = append(eles, ele.([]byte))
	}
	return eles
}

func (impl *zsetImpl) rangeByScore(min, max float64, minex, maxex bool, direct int) [][]byte {
	rs := ds.ScoreRange{min, max, minex, maxex}
	eles := make([][]byte, 0)
	for _, ele := range impl.zsl.RangeByScore(rs) {
		eles = append(eles, ele.([]byte))
	}
	return eles
}

func (impl *zsetImpl) rangeByEle(min, max []byte, minex, maxex bool, direct int) [][]byte {
	rs := ds.EleRange{min, max, minex, maxex}
	eles := make([][]byte, 0)
	for _, ele := range impl.zsl.RangeByEle(rs) {
		eles = append(eles, ele.([]byte))
	}
	return eles
}

func (impl *zsetImpl) countByRank(min, max uint64, minex, maxex bool, direct int) uint64 {
	eles := impl.rangeByRank(min, max, minex, maxex, direct)
	return uint64(len(eles))
}

func (impl *zsetImpl) countByScore(min, max float64, minex, maxex bool, direct int) uint64 {
	eles := impl.rangeByScore(min, max, minex, maxex, direct)
	return uint64(len(eles))
}

func (impl *zsetImpl) countByEle(min, max []byte, minex, maxex bool, direct int) uint64 {
	eles := impl.rangeByEle(min, max, minex, maxex, direct)
	return uint64(len(eles))
}

func (impl *zsetImpl) rank(ele []byte, direct int) uint64 {
	score, ok := impl.dict.Get(string(ele))
	if !ok {
		return 0
	}
	return impl.zsl.GetRank(score.(float64), ele)
}

func (impl *zsetImpl) score(ele []byte) float64 {
	score, ok := impl.dict.Get(string(ele))
	if !ok {
		return 0
	}
	return score.(float64)
}

func (impl *zsetImpl) typ() implType {
	return zsetImplType
}
