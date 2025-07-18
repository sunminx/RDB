package db

import (
	"crypto/rand"
	"encoding/binary"
)

type dict map[string]any

func newDict() dict {
	return dict(make(map[string]any))
}

func (d dict) put(key string, val any) bool {
	_, ok := d[key]
	d[key] = val
	return !ok
}

func (d dict) replace(key string, val any) bool {
	d[key] = val
	return true
}

func (d dict) remove(key string) bool {
	_, ok := d[key]
	if !ok {
		return false
	}
	delete(d, key)
	return true
}

func (d dict) get(key string) (any, bool) {
	val, ok := d[key]
	return val, ok
}

func (d dict) randomKV() (string, any, bool) {
	times := random() % d.used()
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

func (d dict) used() int {
	return len(d)
}

func (d dict) size() int {
	return 0
}
