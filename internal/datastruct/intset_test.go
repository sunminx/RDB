package datastruct

import (
	"math"
	"math/rand/v2"
	"testing"
)

func TestAdd(t *testing.T) {
	testcases := []struct {
		val int64
		enc int
	}{
		{val: int64(math.MaxInt16), enc: IntSetEncInt16},
		{val: int64(math.MinInt16), enc: IntSetEncInt16},
		{val: int64(math.MaxInt32), enc: IntSetEncInt32},
		{val: int64(math.MinInt32), enc: IntSetEncInt32},
		{val: int64(math.MaxInt64), enc: IntSetEncInt64},
		{val: int64(math.MinInt64), enc: IntSetEncInt64},
	}
	is := NewIntSet()
	for _, tc := range testcases {
		if !is.Add(tc.val) || !is.Find(tc.val) {
			t.Errorf("failed to add %d\n", tc.val)
		}
		if is.Encoding() != tc.enc {
			t.Error("invalid encoding")
		}
	}
	if !is.Find(testcases[1].val) {
		t.Error("failed to find")
	}
}

func TestRemove(t *testing.T) {
	testcases := []struct {
		val     int64
		enc     int
		deleted bool
	}{
		{val: int64(math.MaxInt16), enc: IntSetEncInt16, deleted: true},
		{val: int64(math.MinInt16), enc: IntSetEncInt16, deleted: false},
		{val: int64(math.MaxInt32), enc: IntSetEncInt32, deleted: false},
		{val: int64(math.MinInt32), enc: IntSetEncInt32, deleted: true},
		{val: int64(math.MaxInt64), enc: IntSetEncInt64, deleted: false},
		{val: int64(math.MinInt64), enc: IntSetEncInt64, deleted: true},
	}
	is := NewIntSet()
	for _, tc := range testcases {
		is.Add(tc.val)
	}
	for _, tc := range testcases {
		if tc.deleted {
			if !is.Remove(tc.val) {
				t.Error("failed to remove")
			}
		}
	}
	for _, tc := range testcases {
		if tc.deleted && is.Find(tc.val) {
			t.Error("failed to remove")
		}
		if !tc.deleted && !is.Find(tc.val) {
			t.Error("failed to remove")
		}
	}
}

func TestBigLength(t *testing.T) {
	bl := 10000
	nums := make([]int64, bl)
	for i := 0; i < bl; i++ {
		nums[i] = rand.Int64()
	}
	is := NewIntSet()
	for i := 0; i < bl; i++ {
		if !is.Add(nums[i]) {
			t.Error("failed to test biglength")
		}
	}
	for i := bl - 1; i >= 0; i-- {
		if !is.Remove(nums[i]) {
			t.Error("failed to test biglength")
		}
	}
}
