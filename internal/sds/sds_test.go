package sds

import "testing"

func TestSetValue(t *testing.T) {
	v := []byte("hello")
	o := NewRobj(v)
	b := []byte(o.Val().(SDS))
	b[1] = 'E'
	t.Log(string(v))
}
