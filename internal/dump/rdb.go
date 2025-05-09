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

	ds "github.com/sunminx/RDB/internal/datastruct"
	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/hash"
	"github.com/sunminx/RDB/internal/list"
	"github.com/sunminx/RDB/internal/networking"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/rio"
	"github.com/sunminx/RDB/pkg/util"
)

type Rdber struct {
	rd    *rio.Reader
	wr    *rio.Writer
	info  RdbInfo
	cksum int64
	db    *db.DB
	srv   *networking.Server
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

const (
	rdbTypeString uint8 = iota
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
		n := val.Val().(int64)
		enc := encodeInt(n)
		if len(enc) == 0 {
			enc = util.Int64ToBytes(n)
		}
		return rdb.writeRaw(enc)
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

// encodeInt is only used for encoding strings that can be represented as integers.
// which is different from the encoding logic for storing numerical values.
// The first two bit is always rdbEncVal
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
		rdb.saveLen(uint64(ql.Len()))
		node := ql.Head()
		for node != nil {
			list := node.List()
			// todo
			rdb.writeRaw([]byte(*list))
			node = node.Next()
		}
		return saved
	}
	return nosave
}

func (rdb *Rdber) saveHashObject(val *obj.Robj) bool {
	if val.CheckEncoding(obj.ObjEncodingZipmap) {
		zm := val.Val().(*hash.Zipmap)
		rdb.saveLen(uint64(zm.Len()))
		rdb.writeRaw([]byte(*zm.Ziplist))
	}
	return nosave
}

const (
	rdbOpcodeModuleAux    uint8 = iota + 247 /* module auxiliary data. */
	rdbOpcodeIdle                            /* lru idle time. */
	rdbOpcodeFreq                            /* lfu frequency. */
	rdbOpcodeAux                             /* rdb aux field. */
	rdbOpcodeResizedb                        /* hash table resize hint. */
	rdbOpcodeExpiretimeMs                    /* expire time in milliseconds. */
	rdbOpcodeExpiretime                      /* old expire time in seconds. */
	rdbOpcodeSelectdb                        /* db number of the following keys. */
	rdbOpcodeEOF
)

func (rdb *Rdber) saveMillisencondTime(t int64) bool {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, t)
	rdb.saveType(rdbOpcodeExpiretimeMs)
	rdb.writeRaw(buf.Bytes())
	return saved
}

func (rdb *Rdber) saveSelectDBNum(num uint64) bool {
	if !rdb.saveType(rdbOpcodeSelectdb) {
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
	if !rdb.saveType(rdbOpcodeAux) {
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
	ln := uint64(len(str))
	if !rdb.saveLen(ln) {
		return nosave
	}
	return rdb.writeRaw([]byte(str))
}

func (rdb *Rdber) saveCksum() bool {
	sz := unsafe.Sizeof(rdb.cksum)
	buf := make([]byte, sz)
	*(*int64)(unsafe.Pointer(&buf[0])) = rdb.cksum
	rdb.writeRaw(buf)
	return true
}

func (rdb *Rdber) writeRaw(p []byte) bool {
	n, err := rdb.wr.Write(p)
	return err == nil && len(p) == n
}

func (rdb *Rdber) Load() error {
	p := make([]byte, 9, 9)
	if rdb.readRaw(p) != 9 {
		return errors.New("read magic number error")
	}
	if string(p[:5]) != "REDIS" {
		return errors.New("wrong signature trying to load DB from file")
	}
	ver, err := strconv.Atoi(string(p[5:]))
	if err != nil {
		return errors.New("RDB format version not found")
	}
	if ver < 1 || ver > rdb.srv.RdbVersion {
		return fmt.Errorf("can't handle RDB format version %d", ver)
	}

	var expireTime int64 = -1
	var now = time.Now().UnixMilli()
	for {
		loadOpcode := true
		typ := rdb.loadType()
		switch typ {
		case rdbOpcodeExpiretime:
		default:
			loadOpcode = false
		}

		if !loadOpcode {
			o := rdb.genericLoadStringObject()
			if o == nil {
				goto rerr
			}
			k, ok := o.([]byte)
			if !ok {
				goto rerr
			}
			key := string(k)
			val := rdb.loadObject(typ)
			if val == nil {
				goto rerr
			}
			if expireTime != -1 && expireTime < now {
				continue
			}
			rdb.db.SetKey(key, val)
			if expireTime != -1 {
				rdb.db.SetExpire(key, time.Duration(expireTime))
			}

			expireTime = -1
		}
	}
rerr:
	return nil
}

func (rdb *Rdber) loadObject(typ uint8) *obj.Robj {
	switch typ {
	case rdbTypeString:
		return rdb.loadStringObject()
	case rdbTypeList:
		return rdb.loadListObject()
	case rdbTypeHash:
		return rdb.loadHashObject()
	default:
	}
	return nil
}

func (rdb *Rdber) loadHashObject() *obj.Robj {
	ln := rdb.loadLen(nil)
	if ln == rdbLenErr {
		return nil
	}

	v := rdb.genericLoadStringObject()
	zl := ds.Ziplist(v.([]byte))
	zm := hash.Zipmap{&zl}
	robj := obj.NewRobj(zm)
	robj.SetType(obj.ObjHash)
	robj.SetEncoding(obj.ObjEncodingZipmap)
	return robj
}

func (rdb *Rdber) loadListObject() *obj.Robj {
	ln := rdb.loadLen(nil)
	if ln == rdbLenErr {
		return nil
	}

	ql := list.NewQuicklist()
	var i uint64
	for ; i < ln; i++ {
		v := rdb.genericLoadStringObject()
		zl := ds.Ziplist(v.([]byte))
		node := list.CreateQuicklistNode(&zl)
		ql.Link(node)
	}
	robj := obj.NewRobj(ql)
	robj.SetType(obj.ObjList)
	robj.SetEncoding(obj.ObjEncodingQuicklist)
	return robj
}

func (rdb *Rdber) loadStringObject() *obj.Robj {
	v := rdb.genericLoadStringObject()
	if v == nil {
		return nil
	}

	var robj *obj.Robj
	switch v.(type) {
	case int64:
		robj = obj.NewRobj(v)
		robj.SetType(obj.ObjString)
		robj.SetEncoding(obj.ObjEncodingInt)
	case []byte:
		robj = obj.NewRobj(string(v.([]byte)))
		robj.SetType(obj.ObjString)
		robj.SetEncoding(obj.ObjEncodingRaw)
	default:
	}
	return robj
}

func (rdb *Rdber) genericLoadStringObject() any {
	var ln int
	var isEncoded bool

	ln = int(rdb.loadLen(&isEncoded))
	if isEncoded {
		v := rdb.loadStringIntObject(uint8(ln))
		if v == -1 {
			goto rerr
		}
		return v
	} else {
		p := make([]byte, ln, ln)
		if rdb.readRaw(p) != ln {
			goto rerr
		}
		return p
	}
rerr:
	return nil
}

// loadStringIntObject is the inverse operation of encodeInt.
func (rdb *Rdber) loadStringIntObject(typ uint8) int64 {
	var n int64
	var p []byte
	if typ == rdbEncInt8 {
		p = make([]byte, 1, 1)
		if rdb.readRaw(p) != 1 {
			goto err
		}
		n = int64(p[1])
	} else if typ == rdbEncInt16 {
		p = make([]byte, 2, 2)
		if rdb.readRaw(p) != 2 {
			goto err
		}
		n = int64(p[1]) | int64(p[2]<<8)
	} else if typ == rdbEncInt32 {
		p = make([]byte, 4, 4)
		if rdb.readRaw(p) != 4 {
			goto err
		}
		n = int64(p[1]) | int64(p[2]<<8) | int64(p[2]<<16) | int64(p[3]<<24)
	}
	return n
err:
	return -1
}

const rdbLenErr = math.MaxUint64

func (rdb *Rdber) loadLen(isEncoded *bool) uint64 {
	ln, ok := rdb.genericLoadLen()
	if isEncoded != nil {
		*isEncoded = ok
	}
	return ln
}

// genericLoadLen
func (rdb *Rdber) genericLoadLen() (uint64, bool) {
	var n uint64
	var isEncoded bool

	p := make([]byte, 1, 1)
	if rdb.readRaw(p) != 1 {
		goto rerr
	}
	switch uint8((p[0] & 0xc0) >> 6) {
	case rdbEncval:
		isEncoded = true
		// In this case, the last six bits
		// of the first byte are returned which the encoding type.
		n = uint64(p[0] & 0x3f)
	case rdb_6bitlen:
		n = uint64(p[0] & 0x3f)
	case rdb_14bitlen:
		p = append(p, '0')
		if rdb.readRaw(p[1:]) != 1 {
			goto rerr
		}
		n = uint64((p[0]&0x3f)<<8 | p[1])
	case rdb_32bitlen:
		p = make([]byte, 4, 4)
		if rdb.readRaw(p) != 4 {
			goto rerr
		}
		n = uint64(binary.BigEndian.Uint32(p))
	case rdb_64bitlen:
		p = make([]byte, 8, 8)
		if rdb.readRaw(p) != 8 {
			goto rerr
		}
		n = binary.BigEndian.Uint64(p)
	default:
	}
	return n, isEncoded
rerr:
	return rdbLenErr, false
}

func (rdb *Rdber) loadTime() int32 {
	var t int32
	err := binary.Read(rdb.rd, binary.LittleEndian, &t)
	if err != nil {
		return -1
	}
	return t
}

func (rdb *Rdber) loadMillisecondTime() int64 {
	var t int64
	err := binary.Read(rdb.rd, binary.LittleEndian, &t)
	if err != nil {
		return -1
	}
	return t
}

func (rdb *Rdber) loadType() uint8 {
	p := make([]byte, 1, 1)
	if rdb.readRaw(p) != 1 {
		return 0
	}
	return p[0]
}

func (rdb *Rdber) readRaw(p []byte) int {
	n, _ := rdb.rd.Read(p)
	return n
}
