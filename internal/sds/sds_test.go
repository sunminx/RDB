package sds

import (
	"fmt"
	"testing"
)

func TestSetValue(t *testing.T) {
	v := []byte("hello")
	o := NewRobj(v)
	v[1] = 'E'
	o.SetVal(v)
	fmt.Printf("o: %+v", o)
}
