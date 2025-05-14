package dump

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/sunminx/RDB/internal/cmd"
	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/hash"
	"github.com/sunminx/RDB/internal/list"
	"github.com/sunminx/RDB/internal/networking"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/rio"
	"github.com/sunminx/RDB/internal/sds"
	"github.com/sunminx/RDB/pkg/util"
)

type Aofer struct {
	rd      *rio.Reader
	wr      *rio.Writer
	cksum   int64
	db      *db.DB
	fakeCli *networking.Client
}

func (aof *Aofer) rewrite(timestamp int64) error {
	var err error
	for e := range aof.db.Iterator() {
		switch e.Val.Type() {
		case obj.TypeString:
			if !aof.rewriteStringObject(e.Key, e.Val) {
				goto werr
			}
		case obj.TypeList:
			if !aof.rewriteListObject(e.Key, e.Val) {
				goto werr
			}
		case obj.TypeHash:
			if !aof.rewriteHashObject(e.Key, e.Val) {
				goto werr
			}
		default:
		}

		expire := aof.db.Expire(e.Key)
		if expire != -1 {
			cmd := "*3\r\n$9\r\nPEXPIREAT\r\n"
			if _, err = aof.wr.Write([]byte(cmd)); err != nil {
				goto werr
			}
			if !aof.writeBulkString([]byte(e.Key)) {
				goto werr
			}
			if !aof.writeBulkInt(int64(expire)) {
				goto werr
			}
		}
	}
	return nil
werr:
	if err != nil {
		return errors.Join(err, errors.New("failed rewrite aof"))
	}
	return errors.New("failed rewrite aof")
}

func (aof *Aofer) rewriteStringObject(key string, val *obj.Robj) bool {
	cmd := "*3\r\n$3\r\nSET\r\n"
	if _, err := aof.wr.Write([]byte(cmd)); err != nil {
		slog.Warn("failed rewrite string object cmd", "err", err)
		return noRewrite
	}
	if !aof.writeBulkString([]byte(key)) {
		return noRewrite
	}
	if !aof.writeBulkObject(val) {
		return noRewrite
	}
	return rewrited
}

const aofRewriteItemsPerCmd = 64

func (aof *Aofer) rewriteListObject(key string, val *obj.Robj) bool {
	batch, entries := 0, list.Len(val)
	iter := list.NewIterator(val)
	for iter.HasNext() {
		if batch == 0 {
			cmdEntries := util.Cond(entries < aofRewriteItemsPerCmd,
				entries, aofRewriteItemsPerCmd)
			if !aof.writeMultibulkCount(2+cmdEntries) ||
				!aof.writeBulkString([]byte("RPUSH")) ||
				!aof.writeBulkString([]byte(key)) {
				return noRewrite
			}
		}

		entry := iter.Next()
		if !aof.writeBulkString(entry.([]byte)) {
			return noRewrite
		}
		entries--
		batch++
		if batch == aofRewriteItemsPerCmd {
			batch = 0
		}
	}
	return rewrited
}

func (aof *Aofer) rewriteHashObject(key string, val *obj.Robj) bool {
	batch, entries := 0, hash.Len(val)
	iter := hash.NewIterator(val)
	for iter.HasNext() {
		if batch == 0 {
			cmdEntries := util.Cond(entries < aofRewriteItemsPerCmd,
				entries, aofRewriteItemsPerCmd)
			if !aof.writeMultibulkCount(2+cmdEntries*2) ||
				!aof.writeBulkString([]byte("HMSET")) ||
				!aof.writeBulkString([]byte(key)) {
				return noRewrite
			}
		}

		kvPair := iter.Next().(hash.KVPair)
		if !aof.writeBulkString(kvPair[0]) {
			return noRewrite
		}
		if !aof.writeBulkString(kvPair[1]) {
			return noRewrite
		}
		entries--
		batch++
		if batch == aofRewriteItemsPerCmd {
			batch = 0
		}
	}
	return rewrited
}

func (aof *Aofer) writeBulkObject(robj *obj.Robj) bool {
	if robj.CheckEncoding(obj.EncodingInt) {
		return aof.writeBulkInt(robj.Val().(int64))
	} else if robj.CheckEncoding(obj.EncodingRaw) {
		return aof.writeBulkString([]byte(robj.Val().(sds.SDS)))
	}
	slog.Warn("unknown string encoding object")
	return noRewrite
}

func (aof *Aofer) writeBulkInt(n int64) bool {
	bytes := util.Int64ToBytes(n)
	return aof.writeBulkString(bytes)
}

func (aof *Aofer) writeBulkString(s []byte) bool {
	ln := int64(len(s))
	var err error
	if !aof.writeBulkInt(ln) {
		goto werr
	}
	if ln > 0 {
		if _, err = aof.wr.Write(s); err != nil {
			goto werr
		}
	}
	if _, err = aof.wr.Write([]byte("\r\n")); err != nil {
		goto werr
	}
	return rewrited
werr:
	slog.Warn("failed write bulk string in rewrite aof", "err", err)
	return noRewrite
}

func (aof *Aofer) writeMultibulkCount(c int64) bool {
	var err error
	if _, err = aof.wr.Write([]byte{'*'}); err != nil {
		goto werr
	}
	if _, err = aof.wr.Write(util.Int64ToBytes(c)); err != nil {
		goto werr
	}
	if _, err = aof.wr.Write([]byte("\r\n")); err != nil {
		goto werr
	}
	return rewrited
werr:
	slog.Warn("failed write multibulk count", "err", err)
	return noRewrite
}

const aofAnnotationLineMaxLen = 1024

func (aof *Aofer) load() error {
	var err error

	for {
		p, isPrefix, err := aof.rd.ReadLine()
		if isPrefix || err != nil {
			goto rerr
		}
		if p[0] == '#' {
			continue
		}
		if p[0] != '*' {
			err = errors.New("invalid protocol")
			goto rerr
		}
		argc, err := strconv.ParseInt(string(p[1:]), 10, 64)
		if err != nil || argc < 1 {
			goto rerr
		}

		argv := make([][]byte, argc, argc)
		var i int64
		for ; i < argc; i++ {
			p, isPrefix, err := aof.rd.ReadLine()
			if isPrefix || err != nil {
				goto rerr
			}
			if p[0] != '$' {
				goto rerr
			}
			ln, err := strconv.ParseInt(string(p[1:]), 10, 64)
			if err != nil || ln < 1 {
				goto rerr
			}
			p, err = aof.readRaw(int(ln))
			if err != nil {
				goto rerr
			}

			argv[i] = p

			// Discard "CRLF"
			_, err = aof.readRaw(2)
			if err != nil {
				goto rerr
			}
		}

		aof.fakeCli.SetArgument(argv)
		command, err := aof.lookupCommand(string(argv[0]), int(argc))
		if err != nil {
			goto rerr
		}

		if !command.Proc(aof.fakeCli) {
			err = errors.New("failed call command")
			goto rerr
		}
	}
	return nil
rerr:
	return errors.Join(err, errors.New("failed load aof file"))
}

func (aof *Aofer) lookupCommand(name string, argc int) (cmd.Command, error) {
	command, found := aof.fakeCli.Server.LookupCommand(name)
	if found {
		if (command.Arity > 0 && command.Arity != argc) || (argc < -command.Arity) {
			return command, fmt.Errorf("wrong number of arguments for %q command", name)
		}
	}
	return command, nil
}

func (aof *Aofer) readRaw(n int) ([]byte, error) {
	p := make([]byte, n, n)
	if _, err := aof.rd.Read(p); err != nil {
		return nil, err
	}
	return p, nil
}
