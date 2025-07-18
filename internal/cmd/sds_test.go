package cmd

import (
	"fmt"
	"testing"
)

func printBinary(bytes []byte, t *testing.T) {
	var s string
	for _, b := range bytes {
		s += fmt.Sprintf("%08b ", b)
	}
	t.Logf(s)
}

func TestGetByte(t *testing.T) {
	bytes := []byte("12345")
	off := uint64(31)
	target, _ := getByte(bytes, off)
	t.Logf("%08b\n", target)
}

func TestGetOffsetInByte(t *testing.T) {
	off := uint64(30)
	boff := getOffsetInByte(off)
	t.Log(boff)
}

func TestGetBitValue(t *testing.T) {
	v := []byte("12345")
	printBinary(v, t)
	off := uint64(30)
	n := getBit(v, off)
	t.Log(n)
}

func TestSetBitValue(t *testing.T) {
	v := []byte("12345")
	printBinary(v, t)
	off := uint64(29)
	on := uint8(0)
	setBit(v, off, on)
	printBinary(v, t)
	t.Log(string(v))
}

func TestSetSubstring(t *testing.T) {
	str := "Hello redis"
	t.Log(str[:5])
}
