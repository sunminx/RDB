package list

import "testing"

//func TestNewZiplist(t *testing.T) {
//	zl := NewZiplist()
//	t.Log(zl.zlbytes())
//	t.Log(zl.zltail())
//	t.Log(zl.zllen())
//}

func TestBit(t *testing.T) {
	var num int32
	// 32 + 8 + 2
	num = 0b00101010
	t.Log(num & 0x3f)
}
