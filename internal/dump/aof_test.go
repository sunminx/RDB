package dump

import (
	"os"
	"testing"

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
}
