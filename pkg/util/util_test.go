package util

import "testing"

func TestInt64ToBytes(t *testing.T) {
	var n int64 = -9223372036854775808
	dst := Int64ToBytes(n)
	t.Log(string(dst))
}
