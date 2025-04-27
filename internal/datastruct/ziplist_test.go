package list

import (
	"slices"
	"strings"
	"testing"
)

func TestNewZiplist(t *testing.T) {
	zl := NewZiplist()
	zl.Push([]byte("1000"))
	zl.Push([]byte("100000000"))
	zl.Push([]byte(strings.Repeat("hello", 237) + "hel"))
	zl.Push([]byte("1234567"))
	entry, ok := zl.Index(2)
	t.Log(ok)
	t.Log(string(entry))
}

func TestBit(t *testing.T) {
	var num int32
	// 32 + 8 + 2
	num = 0b00101010
	t.Log(num & 0x3f)
}

func TestZipStrSize(t *testing.T) {
	testcases := []struct {
		_type       byte
		encoding    []byte
		lensizeWant int32
		_lenWant    int32
	}{
		{zipStr06b, []byte{0b00111111}, 1, 63},
		{zipStr06b, []byte{0b00000001}, 1, 1},
		{zipStr14b, []byte{0b01000001, 0b00000001}, 2, 257},
		{zipStr14b, []byte{0b01111111, 0b11111111}, 2, 16383},
		{zipStr32b, []byte{0b10000000, 0b01111111, 0b11111111, 0b11111111, 0b11111111}, 5, 2147483647},
	}

	for _, tc := range testcases {
		lensize, _len := zipStrSize(tc._type, tc.encoding)
		if lensize != tc.lensizeWant || _len != tc._lenWant {
			t.Log(_len)
			t.Log(tc._lenWant)
			t.Errorf("zipStrSize: lensize: %d want: %d _len: %d want: %d\n",
				lensize, tc.lensizeWant, _len, tc._lenWant)
		}
	}
}

func TestZipStrEncoding(t *testing.T) {
	testcases := []struct {
		input            []byte
		encodingsizeWant int32
		encoding         []byte
	}{
		{[]byte("hello"), 1, []byte{0b00000101}},
		{[]byte(strings.Repeat("hello", 12) + "hel"), 1, []byte{0b00111111}},
		{[]byte(strings.Repeat("hello", 3276) + "hel"), 2, []byte{0b01111111, 0b11111111}},
		{[]byte(strings.Repeat("hello", 3276) + "hell"), 5, []byte{0b10000000, 0b00000000, 0b00000000, 0b01000000, 0b00000000}},
	}

	for _, tc := range testcases {
		encodingsize, encoding := zipStrEncoding(tc.input)
		t.Logf("%b", encoding)
		if encodingsize != tc.encodingsizeWant ||
			slices.Compare(encoding, tc.encoding) != 0 {
			t.Error("zipStrEncoding")
		}
	}
}

func TestPush(t *testing.T) {
	zl := NewZiplist()
	zl.Push([]byte("hello"))
	zl.PushLeft([]byte("jim"))
	zl.PushLeft([]byte(strings.Repeat("xxx", 3000) + "jij"))
	zl.PushLeft([]byte("1234567"))
	entry, ok := zl.Index(1)
	t.Log(ok)
	t.Log(string(entry))
}

func TestZiplistPop(t *testing.T) {
	zl := NewZiplist()
	zl.Push([]byte("hello"))
	zl.Push([]byte("123456"))
	zl.Push([]byte(strings.Repeat("jim", 2345)))
	zl.PopLeft()
	t.Log(zl.Zllen())
	t.Log(zl.Zlbytes())
	entry, ok := zl.Index(1)
	if ok {
		t.Log(string(entry))
		//t.Log(entry)
	}
}

func TestZiplistWithInt(t *testing.T) {
	zl := NewZiplist()
	zl.Push([]byte("166"))
	entry, ok := zl.Index(0)
	if ok {
		t.Log(string(entry))
	}
}

func TestZiplistRemove(t *testing.T) {
	zl := NewZiplist()
	zl.Push([]byte("1"))
	zl.Push([]byte("2"))
	zl.Push([]byte("3"))
	zl.Push([]byte("4"))
	zl.Push([]byte("5"))
	zl.RemoveTail(1, 2)
	entry, _ := zl.Index(0)
	t.Log(string(entry))
}

func TestZiplistReplaceAtIndex(t *testing.T) {
	zl := NewZiplist()
	zl.Push([]byte("111111"))
	entry, _ := zl.Index(0)
	t.Log(string(entry))
	zl.ReplaceAtIndex(0, []byte("2"))
	entry, _ = zl.Index(0)
	t.Log(string(entry))
}
