package db

import (
	"sync"

	"github.com/sunminx/RDB/internal/dict"
)

type DB struct {
	sync.RWMutex
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
	return &DB{sync.RWMutex{}, dict.NewMap(), dict.NewMap()}
}

func (db *DB) LookupKeyRead(key string) (dict.Robj, bool) {
	return db.lookupKey(key)
}

func (db *DB) LookupKeyWrite(key string) (dict.Robj, bool) {
	// todo
	return db.lookupKey(key)
}

func (db *DB) lookupKey(key string) (dict.Robj, bool) {
	db.RLock()
	defer db.RUnlock()
	val, ok := db.dict.FetchValue(key)
	if ok {
		// todo
		return val, true
	}

	return dict.Robj{}, false
}

func (db *DB) SetKey(key string, val dict.Robj) {
	db.Lock()
	defer db.Unlock()
	_ = val.TryObjectEncoding()

	if _, ok := db.dict.FetchValue(key); ok {
		db.dict.Replace(key, val)
	} else {
		db.dict.Add(key, val)
	}
}

func (db *DB) DelKey(key string) {
	db.Lock()
	defer db.Unlock()
	db.dict.Del(key)
}
