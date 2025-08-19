package datastruct

import (
	"strconv"
	"testing"
)

func TestRandomKV(t *testing.T) {
	d := NewDict()
	cnt := 10000
	for i := 0; i < cnt; i++ {
		d.Put(strconv.Itoa(i), i)
	}

	for i := 0; i < cnt; i++ {
		_, _, ok := d.RandomKV()
		if !ok {
			t.Error("failed to get random kv")
		}
	}
}
