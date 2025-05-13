package dump

import (
	"os"
	"sync"
	"testing"

	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/hash"
	"github.com/sunminx/RDB/internal/list"
	"github.com/sunminx/RDB/internal/networking"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/rio"
	"github.com/sunminx/RDB/internal/sds"
)

var mdb *db.DB
var once sync.Once

func initDB() *db.DB {
	once.Do(func() {
		mdb = newMockDB()
	})
	return mdb
}

func newMockDB() *db.DB {
	mdb := db.New()
	key := "key1"
	val := obj.New(sds.SDS("val1"), obj.TypeString, obj.EncodingRaw)
	mdb.SetKey(key, val)
	return mdb
}

func newMockRdb(t *testing.T) *Rdber {
	mdb := initDB()
	file, err := os.Create("./rdb.file")
	if err != nil {
		t.Error("cannot create rdb.file")
	}
	wr, err := rio.NewWriter(file)
	if err != nil {
		t.Error(err)
	}
	file, err = os.Open("./rdb.file")
	if err != nil {
		t.Error("cannot create rdb.file")
	}
	rd, err := rio.NewReader(file)
	if err != nil {
		t.Error(err)
	}
	srv := networking.NewServer()
	return &Rdber{
		wr:   wr,
		rd:   rd,
		db:   mdb,
		info: newRdberInfo(srv),
	}
}

func TestSaveLoadLen(t *testing.T) {
	rdb := newMockRdb(t)
	testcases := []uint64{10, 100, 1000, 10000, 1000000, 100000000, 10000000000}
	for _, tc := range testcases {
		if !rdb.saveLen(tc) {
			t.Error("save len error")
		}
		n := rdb.loadLen(nil)
		if tc != n {
			t.Errorf("test saveLen & loadLen failed, "+
				"save: %d, load: %d", tc, n)
		}
	}
}

func TestSaveLoadRawString(t *testing.T) {
	rdb := newMockRdb(t)
	testcases := []string{"hello", "100"}
	for _, tc := range testcases {
		if !rdb.saveRawString(tc) {
			t.Error("save raw string error")
		}
		v := rdb.genericLoadStringObject()
		s, ok := v.([]byte)
		if !ok {
			t.Error("save raw string error")
		} else if tc != string(s) {
			t.Errorf("test saveRawString & loadRawString failed, "+
				"save: %s, load: %v", tc, v)
		}
	}
}

func TestSaveLoadStringObject(t *testing.T) {
	rdb := newMockRdb(t)
	testcases := []int64{100000}
	for _, tc := range testcases {
		robj := obj.New(tc, obj.TypeString, obj.EncodingInt)
		if !rdb.saveStringObject(robj) {
			t.Error("save string object error")
		}
		rrobj := rdb.loadStringObject()
		if !rrobj.CheckType(robj.Type()) {
			t.Error("type")
		}
		if !rrobj.CheckEncoding(robj.Encoding()) {
			t.Error("encoding")
		}
		if robj.Val() != rrobj.Val() {
			t.Log(rrobj.Val())
			t.Error("val")
		}
	}
}

func TestSaveLoadListObject(t *testing.T) {
	rdb := newMockRdb(t)
	li := list.NewQuicklist()
	li.Push([]byte("hello"))
	robj := obj.New(li, obj.TypeList, obj.EncodingQuicklist)
	if !rdb.saveListObject(robj) {
		t.Error("save quicklist error")
	}
	rrobj := rdb.loadListObject()
	if rrobj == nil {
		t.Error("load quicklist error")
	}
	v, ok := rrobj.Val().(*list.Quicklist)
	if !ok {
		t.Error("load quicklist error")
	}
	item, _ := v.Index(0)
	if string(item) != "hello" {
		t.Error("load quicklist error")
	}
}

func TestSaveLoadHashObject(t *testing.T) {
	rdb := newMockRdb(t)

	zm := hash.NewZipmap()
	hmap := hash.NewRobj(zm)
	hash.Set(hmap, []byte("key"), []byte("val"))
	if !rdb.saveHashObject(hmap) {
		t.Error("save hash object error 1")
	}
	robj := rdb.loadHashObject()
	if robj == nil {
		t.Error("load hash object error 2")
	}
	val, ok := hash.Get(robj, []byte("key"))
	if !ok {
		t.Error("load hash object error 4")
	}
	if string(val) != "val" {
		t.Error(string(val))
		t.Error("load hash object error 3")
	}
}
