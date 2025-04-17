package db

import (
	"github.com/sunminx/RDB/internal/dict"
)

type DB struct {
	dict    Dicter
	expires Dicter
}

type Dicter interface {
	Add(string, dict.Robj) bool
	Replace(string, dict.Robj) bool
	Del(string) bool
	FetchValue(string) (dict.Robj, bool)
	GetRandomKey() dict.Entry
	Used() int
	Size() int
}

func New() *DB {
	return &DB{
		dict:    dict.NewMap(),
		expires: dict.NewMap(),
	}
}

func (db *DB) LookupKeyRead(key string) (dict.Robj, bool) {
	return db.lookupKey(key)
}

func (db *DB) LookupKeyWrite(key string) (dict.Robj, bool) {
	// todo
	return db.lookupKey(key)
}

func (db *DB) lookupKey(key string) (dict.Robj, bool) {
	val, ok := db.dict.FetchValue(key)
	if ok {
		// todo
		return val, true
	}

	return dict.Robj{}, false
}

func (db *DB) SetKey(key string, val dict.Robj) {
	_ = val.TryObjectEncoding()

	if _, ok := db.dict.FetchValue(key); ok {
		db.dict.Replace(key, val)
	} else {
		db.dict.Add(key, val)
	}
}

func (db *DB) DelKey(key string) {
	db.dict.Del(key)
}
