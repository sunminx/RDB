package dump

import (
	"os"
	"testing"

	"github.com/sunminx/RDB/internal/hash"
	"github.com/sunminx/RDB/internal/list"
	"github.com/sunminx/RDB/internal/networking"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/rio"
	"github.com/sunminx/RDB/internal/sds"
)

func newMockAof(t *testing.T) *Aofer {
	file, err := os.Create("aof.file")
	if err != nil {
		t.Error(err)
	}
	wr, err := rio.NewWriter(file)
	if err != nil {
		t.Error(err)
	}
	file, err = os.Open("aof.file")
	if err != nil {
		t.Error(err)
	}
	rd, err := rio.NewReader(file)
	if err != nil {
		t.Error(err)
	}
	db := newMockDB()
	srv := networking.NewServer()
	srv.Init()
	cli := networking.NewClient(nil, db)
	cli.Server = srv
	return &Aofer{
		file:    file,
		rd:      rd,
		wr:      wr,
		db:      db,
		fakeCli: cli,
	}
}

func TestAofRewriteLoadStringObject(t *testing.T) {
	aof := newMockAof(t)
	key := "key1"
	val := obj.New(sds.SDS("val1"), obj.TypeString, obj.EncodingRaw)
	if !aof.rewriteStringObject(key, val) {
		t.Error("failed rewrite string object")
	}
	srv := aof.fakeCli.Server
	ret := aof.loadSingleFile("./aof.file", srv)
	if ret != aofOk && ret != aofTruncated {
		t.Error("failed load AOF file")
	}
	robj, found := aof.db.LookupKeyRead("key1")
	if !found {
		t.Error("failed read key-val")
	}
	t.Log(string(robj.Val().(sds.SDS)))
}

func TestAofRewriteLoadListObject(t *testing.T) {
	aof := newMockAof(t)
	key := "key2"
	ql := list.NewQuicklist()
	ql.Push([]byte("hello"))
	val := obj.New(ql, obj.TypeList, obj.EncodingQuicklist)
	if !aof.rewriteListObject(key, val) {
		t.Error("failed rewrite string object")
	}
	srv := aof.fakeCli.Server
	ret := aof.loadSingleFile("./aof.file", srv)
	if ret != aofOk && ret != aofTruncated {
		t.Error("failed load AOF file")
	}
	robj, found := aof.db.LookupKeyRead("key2")
	if !found {
		t.Error("failed read key-val")
	}
	v, ok := robj.Val().(*list.Quicklist).Index(0)
	if !ok {
		t.Error("failed read entry")
	}
	t.Log(string(v))
}

func TestAofRewriteLoadHashObject(t *testing.T) {
	aof := newMockAof(t)
	key := "key3"
	zm := hash.NewZipmap()
	val := hash.NewRobj(zm)
	hash.Set(val, []byte("key"), []byte("val"))
	if !aof.rewriteHashObject(key, val) {
		t.Error("failed rewrite string object")
	}
	srv := aof.fakeCli.Server
	ret := aof.loadSingleFile("./aof.file", srv)
	if ret != aofOk && ret != aofTruncated {
		t.Error("failed load AOF file")
	}
	robj, found := aof.db.LookupKeyRead("key3")
	if !found {
		t.Error("failed read key-val")
	}
	v, _ := hash.Get(robj, []byte("key"))
	t.Log(string(v))
}
