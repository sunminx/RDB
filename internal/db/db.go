package main

import (
	"errors"

	"github.com/sunminx/RDB/internal/dict"
	"github.com/sunminx/RDB/internal/server"
)

type DB struct {
	dict    Dicter
	expires Dicter
}

type Dicter interface {
	Add(string, dict.Robj) bool
	Replace(string, dict.Robj) bool
	Del(string) bool
	Get(string) (dict.Robj, bool)
}

func New() *DB {
	return &DB{
		dict:    dict.NewMap(),
		expires: dict.NewMap(),
	}
}

func (db *DB) LookupKeyReadOrReply(client *server.Client, key string) (dict.Robj, error) {
	val, err := db.LookupRead(key)
	if err != nil {
		// todo
	}
	return val, err
}

func (db *DB) LookupKeyRead(key string) (dict.Robj, error) {
	var err error
	val, ok := db.dict.Get(key)
	if !ok {
		err = errors.New("cannot lookup for " + key)
	}
	return val, err
}
