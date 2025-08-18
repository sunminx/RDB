package datastruct

import (
	"crypto/rand"
	"encoding/binary"
)

type Dict map[string]any

func NewDict() Dict {
	return Dict(make(map[string]any))
}

func (d Dict) Put(key string, val any) bool {
	_, ok := d[key]
	d[key] = val
	return !ok
}

func (d Dict) Replace(key string, val any) bool {
	d[key] = val
	return true
}

func (d Dict) Remove(key string) bool {
	_, ok := d[key]
	if !ok {
		return false
	}
	delete(d, key)
	return true
}

func (d Dict) Get(key string) (any, bool) {
	val, ok := d[key]
	return val, ok
}

func (d Dict) RandomKV() (string, any, bool) {
	times := random() % d.Used()
	n := 0
	for key, val := range d {
		if n == times {
			return key, val, true
		}
		n++
	}
	return "", nil, false
}

func random() int {
	buf := make([]byte, 4)
	_, err := rand.Read(buf)
	if err != nil {
		return 0
	}
	n := int(binary.LittleEndian.Uint32(buf[:]))
	if n < 0 {
		return -n
	}
	return n
}

func (d Dict) Used() int {
	return len(d)
}

func (d Dict) Size() int {
	return 0
}
