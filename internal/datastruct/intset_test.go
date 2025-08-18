package datastruct

import (
	"math"
	"testing"
)

func TestAdd(t *testing.T) {
	testcases := []struct {
		val int64
		enc int
	}{
		{val: int64(math.MaxInt16 - 1), enc: IntSetEncInt16},
		{val: int64(math.MaxInt32 - 1), enc: IntSetEncInt32},
		{val: int64(math.MaxInt64 - 1), enc: IntSetEncInt64},
	}
	is := NewIntSet()
	for _, tc := range testcases {
		if !is.Add(tc.val) || !is.Find(tc.val) {
			t.Error("failed to add")
		}
		if is.Encoding() != tc.enc {
			t.Error("invalid encoding")
		}
	}
	if !is.Find(testcases[1].val) {
		t.Error("failed to find")
	}
}
