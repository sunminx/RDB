package dump

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
	"unsafe"

	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/hash"
	"github.com/sunminx/RDB/internal/list"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/rio"
	"github.com/sunminx/RDB/pkg/util"
)

type Rdber struct {
	rd    *rio.Reader
	wr    *rio.Writer
	info  RdbInfo
	cksum int64
}

type RdbInfo struct {
	Version int
}

func (rdb *Rdber) Save(db *db.DB) error {
	if !rdb.writeRaw([]byte(fmt.Sprintf("REDIS%04d", rdb.info.Version))) {
		return errors.New("write rdb version error")
	}
	if !rdb.saveAuxFields() {
		return errors.New("save aux field error")
	}
	// In our implementation, there is always only one db.
	if !rdb.saveSelectDBNum(0) {
		return errors.New("save select db num error")
	}

	for e := range db.Iterator() {
		if !rdb.saveKeyValPair(e.Key, e.Val, e.Expire) {
			return errors.New("save key-val pair error")
		}
	}

	if !rdb.saveType(rdbOpcodeEOF) {
		return errors.New("save EOF error")
	}
	if !rdb.saveCksum() {
		return errors.New("save checksum error")
	}
	return nil
}

func (rdb *Rdber) saveKeyValPair(key string, val *obj.Robj, expire int64) bool {
	var saved = true
	if expire != -1 {
		saved = saved && rdb.saveMillisencondTime(expire)
	}
	saved = saved && rdb.saveObjectType(val)
	saved = saved && rdb.saveRawString(key)
	saved = saved && rdb.saveObject(val)
	return saved
}

type rdbType uint8

const (
	rdbTypeString rdbType = iota
	rdbTypeList
	rdbTypeSet
	rdbTypeZset
	rdbTypeHash
	rdbTypeZset_2 /* zset version 2 with doubles stored in binary. */
	rdbTypeModule

	rdbTypeHashZipmap = 9
	rdbTypeListZiplist
	rdbTypeSetIntset
	rdbTypeZsetZiplist
	rdbTypeHashZiplist
	rdbTypeListQuicklist
	rdbTypeStreamListpacks
)

const (
	saved  = true
	nosave = false
)

func (rdb *Rdber) saveObjectType(val *obj.Robj) bool {
	switch val.Type() {
	case obj.ObjString:
		rdb.saveType(rdbTypeString)
		return saved
	case obj.ObjList:
		if val.CheckEncoding(obj.ObjEncodingQuicklist) {
			rdb.saveType(rdbTypeListQuicklist)
		}
		return saved
	case obj.ObjHash:
		if val.CheckEncoding(obj.ObjEncodingZipmap) {
			rdb.saveType(rdbTypeHashZipmap)
		}
		return saved
	default:
	}
	return nosave
}

func (rdb *Rdber) saveObject(val *obj.Robj) bool {
	switch val.Type() {
	case obj.ObjString:
		return rdb.saveStringObject(val)
	case obj.ObjList:
		return rdb.saveListObject(val)
	case obj.ObjHash:
		return rdb.saveHashObject(val)
	default:
	}
	return nosave
}

func (rdb *Rdber) saveStringObject(val *obj.Robj) bool {
	if val.CheckEncoding(obj.ObjEncodingInt) {
		// todo
		n := val.Val().(int64)
		enc := encodeInt(n)
		if len(enc) > 0 {
			return rdb.writeRaw(enc)
		}

		return saved
	} else if val.CheckEncoding(obj.ObjEncodingRaw) {
		rdb.saveRawString(val.Val().(string))
		return saved
	}
	return nosave
}

const (
	rdbEncval = 3

	rdbEncInt8  = 0 /* 8 bit signed integer */
	rdbEncInt16 = 1 /* 16 bit signed integer */
	rdbEncInt32 = 2 /* 32 bit signed integer */
	rdbEncLzf   = 3
)

func encodeInt(n int64) []byte {
	var enc []byte
	if n >= math.MinInt8 && n <= math.MaxInt8 {
		enc = make([]byte, 2, 2)
		enc[0] = (rdbEncval << 6) | rdbEncInt8
		enc[1] = byte(n & 0xff)
	} else if n >= math.MinInt16 && n <= math.MaxInt16 {
		enc = make([]byte, 3, 3)
		enc[0] = (rdbEncval << 6) | rdbEncInt16
		enc[1] = byte(n & 0xff)
		enc[2] = byte((n >> 8) & 0xff)
	} else if n >= math.MinInt32 && n <= math.MaxInt32 {
		enc = make([]byte, 5, 5)
		enc[0] = (rdbEncval << 6) | rdbEncInt32
		enc[1] = byte(n & 0xff)
		enc[2] = byte((n >> 8) & 0xff)
		enc[3] = byte((n >> 16) & 0xff)
		enc[4] = byte((n >> 24) & 0xff)
	}
	return enc
}

func (rdb *Rdber) saveListObject(val *obj.Robj) bool {
	if val.CheckEncoding(obj.ObjEncodingQuicklist) {
		ql := val.Val().(*list.Quicklist)
		rdb.saveLen(ql.Len())
		node := ql.Head()
		for node != nil {
			list := node.List()
			// todo
			rdb.writeRaw(list)
			node = node.Next()
		}
		return saved
	}
	return nosave
}

func (rdb *Rdber) saveHashObject(val *obj.Robj) bool {
	if val.CheckEncoding(obj.ObjEncodingZipmap) {
		zm := val.Val().(*hash.Zipmap)
		rdb.writeRaw(zm)
	}
	return nosave
}

type Opcode uint8

const (
	RdbOpcodeModuleAux    Opcode = iota + 247 /* module auxiliary data. */
	RdbOpcodeIdle                             /* lru idle time. */
	RdbOpcodeFreq                             /* lfu frequency. */
	RdbOpcodeAux                              /* rdb aux field. */
	RdbOpcodeResizedb                         /* hash table resize hint. */
	RdbOpcodeExpiretimeMs                     /* expire time in milliseconds. */
	RdbOpcodeExpiretime                       /* old expire time in seconds. */
	RdbOpcodeSelectdb                         /* db number of the following keys. */
	RdbOpcodeEOF
)

func (rdb *Rdber) saveMillisencondTime(t int64) bool {
	var buf bytes.Buffer
	binary.Write(buf, binary.LittleEndian, t)
	rdb.saveType(RdbOpcodeExpiretimeMs)
	rdb.writeRaw(buf.Bytes())
	return saved
}

func (rdb *Rdber) saveSelectDBNum(num uint64) bool {
	if !rdb.saveType(RdbOpcodeSelectdb) {
		return false
	}
	return rdb.saveLen(num)
}

func (rdb *Rdber) saveAuxFields() bool {
	var saved bool = true
	saved = saved && rdb.saveAuxFieldStrStr("redis-ver", "9")
	saved = saved && rdb.saveAuxFieldStrInt("redis-bits", util.Cond(unsafe.Sizeof(uintptr(0)) == 4, int64(32), int64(64)))
	saved = saved && rdb.saveAuxFieldStrInt("ctime", time.Now().Unix())
	saved = saved && rdb.saveAuxFieldStrInt("used-mem", 0)
	return saved
}

func (rdb *Rdber) saveAuxFieldStrStr(key, val string) bool {
	return rdb.saveAuxField(key, val)
}

func (rdb *Rdber) saveAuxFieldStrInt(key string, val int64) bool {
	return rdb.saveAuxField(key, strconv.FormatInt(val, 10))
}

func (rdb *Rdber) saveAuxField(key, val string) bool {
	if !rdb.saveType(RdbOpcodeAux) {
		return false
	}
	if !rdb.saveRawString(key) {
		return false
	}
	return rdb.saveRawString(val)
}

func (rdb *Rdber) saveType(t uint8) bool {
	return rdb.writeRaw([]byte{t})
}

const (
	rdb_6bitlen  = 0
	rdb_14bitlen = 1
	rdb_32bitlen = 0x80
	rdb_64bitlen = 0x81
)

func (rdb *Rdber) saveLen(ln uint64) bool {
	var buf = make([]byte, 1, 1)

	switch {
	case ln < (1 << 6):
		buf[0] = uint8(ln&0xff) | (rdb_6bitlen << 6)
		return rdb.writeRaw(buf)
	case ln < (1 << 14):
		buf = make([]byte, 2, 2)
		buf[0] = uint8((ln>>8)&0xff) | (rdb_14bitlen << 6)
		buf[1] = uint8(ln & 0xff)
		return rdb.writeRaw(buf)
	case ln < math.MaxUint32:
		buf[0] = rdb_32bitlen
		if !rdb.writeRaw(buf) {
			return false
		}
		buf = make([]byte, 4, 4)
		binary.BigEndian.PutUint32(buf, uint32(ln))
		return rdb.writeRaw(buf)
	default:
		buf[0] = rdb_64bitlen
		if !rdb.writeRaw(buf) {
			return false
		}
		buf = make([]byte, 8, 8)
		binary.BigEndian.PutUint64(buf, ln)
		return rdb.writeRaw(buf)
	}
}

func (rdb *Rdber) saveRawString(str string) bool {
	return rdb.writeRaw([]byte(str))
}

func (rdb *Rdber) saveCksum() bool {
	sz := unsafe.Sizeof(rdb.cksum)
	buf := make([]byte, sz)
	*(*int64)(unsafe.Pointer(&buf[0])) = n
	rdb.writeRaw(buf)
	return true
}

func (rdb *Rdber) writeRaw(p []byte) bool {
	n, err := rdb.wr.Write(p)
	return err == nil && len(p) == n
}
