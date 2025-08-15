package datastruct

import (
	"slices"
	"testing"

	"math/rand/v2"
)

func TestInsert(t *testing.T) {
	zsl := NewSkiplist(StrCompare)
	n := 1000000
	for i := 0; i < n; i++ {
		score := randScore()
		ele := randEle()
		if !zsl.Insert(score, ele) {
			t.Error("failed to insert")
		}
	}
	t.Logf("zsl length=%d\n", zsl.Length())
}

func TestDelete(t *testing.T) {
	zsl := NewSkiplist(StrCompare)
	n := 1000000
	scores := make([]float64, n)
	eles := make([]string, n)
	for i := 0; i < n; i++ {
		scores[i] = randScore()
		eles[i] = randEle()
	}
	for i := 0; i < n; i++ {
		if !zsl.Insert(scores[i], eles[i]) {
			t.Error("failed to insert")
		}
	}
	t.Logf("zsl length=%d\n", zsl.Length())
	for i := 0; i < n; i++ {
		if !zsl.Delete(scores[i], eles[i]) {
			t.Error("failed to delete")
		}
	}
	t.Logf("zsl length=%d\n", zsl.Length())
}

func TestUpdate(t *testing.T) {
	zsl := NewSkiplist(StrCompare)
	n := 1000000
	scores := make([]float64, n)
	eles := make([]string, n)
	newscores := make([]float64, n)
	for i := 0; i < n; i++ {
		scores[i] = randScore()
		eles[i] = randEle()
		newscores[i] = randScore()
	}
	for i := 0; i < n; i++ {
		if !zsl.Insert(scores[i], eles[i]) {
			t.Error("failed to insert")
		}
	}
	t.Logf("zsl length=%d\n", zsl.Length())
	for i := 0; i < n; i++ {
		if zsl.Update(scores[i], eles[i], newscores[i]) == nil {
			t.Error("failed to update score")
		}
	}
	t.Logf("zsl length=%d\n", zsl.Length())
}

func TestGetRank(t *testing.T) {
	zsl := NewSkiplist(StrCompare)
	n := 1000000
	scores := make([]float64, n)
	eles := make([]string, n)
	for i := 0; i < n; i++ {
		scores[i] = randScore()
		eles[i] = randEle()
	}
	slices.Sort(scores)
	slices.Sort(eles)
	for i := 0; i < n; i++ {
		if !zsl.Insert(scores[i], eles[i]) {
			t.Error("failed to insert")
		}
	}
	t.Logf("zsl length=%d\n", zsl.Length())
	for i := 0; i < n; i++ {
		if zsl.GetRank(scores[i], eles[i]) != uint64(i+1) {
			t.Logf("score: %f ele: %v idx: %d rank: %d",
				scores[i], eles[i], i, zsl.GetRank(scores[i], eles[i]))
			t.Error("failed to get rank")
		}
	}
	t.Logf("zsl length=%d\n", zsl.Length())
}

func TestDeleteByScore(t *testing.T) {
	zsl := NewSkiplist(StrCompare)
	n := 1000000
	scores := make([]float64, n)
	eles := make([]string, n)
	for i := 0; i < n; i++ {
		scores[i] = randScore()
		eles[i] = randEle()
	}
	slices.Sort(scores)
	slices.Sort(eles)
	for i := 0; i < n; i++ {
		zsl.Insert(scores[i], eles[i])
	}
	t.Logf("zsl length: %d\n", zsl.length)
	lo, hi := 10, 1000
	rs := scoreRange{min: scores[lo], max: scores[hi], minex: false, maxex: false}
	_ = zsl.DeleteRangeByScore(rs)
	t.Logf("zsl length: %d\n", zsl.length)
	for lo > 0 {
		if scores[lo-1] == rs.min {
			lo--
		} else {
			break
		}
	}
	for hi < n-1 {
		if scores[hi+1] == rs.max {
			hi++
		} else {
			break
		}
	}
	for i := 0; i < lo; i++ {
		if zsl.GetRank(scores[i], eles[i]) == 0 {
			t.Error("failed to delete by score")
		}
	}
	for i := lo; i <= hi; i++ {
		if zsl.GetRank(scores[i], eles[i]) != 0 {
			t.Logf("failed: idx: %d score %f ele %v min: %f max: %f",
				i, scores[i], eles[i], rs.min, rs.max)
			t.Error("failed to delete by score")
		}
	}
	for i := hi + 1; i < n; i++ {
		if zsl.GetRank(scores[i], eles[i]) == 0 {
			t.Error("failed to delete by score")
		}
	}
}

func TestDeleteByRank(t *testing.T) {
	zsl := NewSkiplist(StrCompare)
	n := 1000000
	scores := make([]float64, n)
	eles := make([]string, n)
	for i := 0; i < n; i++ {
		scores[i] = randScore()
		eles[i] = randEle()
	}
	slices.Sort(scores)
	slices.Sort(eles)
	for i := 0; i < n; i++ {
		zsl.Insert(scores[i], eles[i])
	}
	t.Logf("zsl length: %d\n", zsl.length)
	lo, hi := 10, 100000
	rs := rankRange{min: uint64(lo), max: uint64(hi), minex: false, maxex: false}
	_ = zsl.DeleteRangeByRank(rs)
	t.Logf("zsl length: %d\n", zsl.length)
	for i := 0; i < lo-1; i++ {
		if zsl.GetRank(scores[i], eles[i]) == 0 {
			t.Logf("failed: idx: %d score %f ele %v min: %d max: %d, rank: %d\n",
				i, scores[i], eles[i], rs.min, rs.max, zsl.GetRank(scores[i], eles[i]))
			t.Error("failed to delete by score")
		}
	}
	for i := lo - 1; i < hi; i++ {
		if zsl.GetRank(scores[i], eles[i]) != 0 {
			t.Logf("failed: idx: %d score %f ele %v min: %d max: %d",
				i, scores[i], eles[i], rs.min, rs.max)
			t.Error("failed to delete by score")
		}
	}
	for i := hi; i < n; i++ {
		if zsl.GetRank(scores[i], eles[i]) == 0 {
			t.Error("failed to delete by score")
		}
	}
}

func TestVisualize(t *testing.T) {
	zsl := NewSkiplist(StrCompare)
	n := 20
	for i := 0; i < n; i++ {
		score := randScore()
		ele := randEle()
		zsl.Insert(score, ele)
	}
	t.Logf("zsl length=%d\n", zsl.Length())
	zsl.visualize()
}

func randScore() float64 {
	return float64(rand.Uint64() & 0xFFFF)
}

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	length        = 12
)

func randEle() string {
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.IntN(len(letterBytes))]
	}
	return string(b)
}
