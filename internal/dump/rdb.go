package dump

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
	"unsafe"

	ds "github.com/sunminx/RDB/internal/datastruct"
	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/hash"
	"github.com/sunminx/RDB/internal/list"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/rio"
	"github.com/sunminx/RDB/internal/sds"
	"github.com/sunminx/RDB/pkg/util"
)

type Rdber struct {
	rd    *rio.Reader
	wr    *rio.Writer
	cksum int64
	db    *db.DB
	info  rdberInfo
}

func newRdbSaver(file *os.File, mode byte, db *db.DB, rdberInfo rdberInfo) (*Rdber, error) {
	rdber := Rdber{db: db, info: rdberInfo}
	if mode == 'r' {
		rd, err := rio.NewReader(file)
		if err != nil {
			return nil, errors.Join(err, errors.New("can't create rio reader"))
		}
		rdber.rd = rd
		return &rdber, nil
	} else if mode == 'w' {
		wr, err := rio.NewWriter(file)
		if err != nil {
			return nil, errors.Join(err, errors.New("can't create rio writer"))
		}
		rdber.wr = wr
		return &rdber, nil
	}
	return nil, errors.New("invalide mode")
}

func (rdb *Rdber) save(ctx context.Context) error {
	if !rdb.writeRaw([]byte(fmt.Sprintf("REDIS%04d", rdb.info.version))) {
		return errors.New("write rdb version error")
	}
	if !rdb.saveAuxFields() {
		return errors.New("save aux field error")
	}
	// In our implementation, there is always only one db.
	if !rdb.saveSelectDBNum(0) {
		return errors.New("save select db num error")
	}

	for e := range rdb.db.Iterator() {
		select {
		case <-ctx.Done():
			return errContextCanceled
		default:
			saved := rdb.saveKeyValPair(e.Key, e.Val, e.Expire)
			if !saved {
				return errors.New("save key-val pair error")
			}
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

func (rdb *Rdber) saveSelectDBNum(num uint64) bool {
	if !rdb.saveType(rdbOpcodeSelectdb) {
		return false
	}
	return rdb.saveLen(num)
}

func (rdb *Rdber) loadSelectDBNum() uint64 {
	return rdb.loadLen(nil)
}

func (rdb *Rdber) saveAuxFields() bool {
	var saved bool = true
	saved = saved && rdb.saveAuxFieldStrStr("redis-ver", "9")
	saved = saved && rdb.saveAuxFieldStrInt("redis-bits", util.Cond(unsafe.Sizeof(uintptr(0)) == 4, int64(32), int64(64)))
	saved = saved && rdb.saveAuxFieldStrInt("ctime", time.Now().Unix())
	saved = saved && rdb.saveAuxFieldStrInt("used-mem", 0)
	return saved
}

func (rdb *Rdber) loadAuxField() (string, any) {
	key := rdb.genericLoadStringObject()
	val := rdb.genericLoadStringObject()
	return string(key.([]byte)), val
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

const (
	rdb_6bitlen  = 0
	rdb_14bitlen = 1
	rdb_32bitlen = 0x80
	rdb_64bitlen = 0x81
)

func (rdb *Rdber) saveCksum() bool {
	sz := unsafe.Sizeof(rdb.cksum)
	buf := make([]byte, sz)
	*(*int64)(unsafe.Pointer(&buf[0])) = rdb.cksum
	rdb.writeRaw(buf)
	return true
}

func (rdb *Rdber) load() error {
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
	if ver < 1 || ver > rdb.info.version {
		return fmt.Errorf("can't handle RDB format version %d", ver)
	}

	var expireTime int64 = -1
	var now = time.Now().UnixMilli()
loop:
	for {
		loadOpcode := true
		typ := rdb.loadType()
		switch typ {
		case rdbOpcodeExpiretime:
		case rdbOpcodeAux:
			_, _ = rdb.loadAuxField()
		case rdbOpcodeSelectdb:
			_ = rdb.loadSelectDBNum()
		case rdbOpcodeEOF:
			break loop
		default:
			loadOpcode = false
		}

		if !loadOpcode {
			o := rdb.genericLoadStringObject()
			if o == nil {
				return errors.New("failed load key in RDB file")
			}
			key := string(o.([]byte))
			val := rdb.loadObject(typ)
			if val == nil {
				return errors.New("failed load val in RDB file")
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
	return nil
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

func (rdb *Rdber) loadObject(typ uint8) *obj.Robj {
	switch typ {
	case rdbTypeString:
		return rdb.loadStringObject()
	case rdbTypeList:
		return rdb.loadListObject()
	case rdbTypeHash:
		return rdb.loadHashObject()
	default:
		return nil
	}
}

func (rdb *Rdber) loadHashObject() *obj.Robj {
	ln := rdb.loadLen(nil)
	if ln == rdbLenErr {
		return nil
	}
	v := rdb.genericLoadStringObject()
	zl := ds.Ziplist(v.([]byte))
	zm := &hash.Zipmap{Ziplist: &zl}
	return obj.New(zm, obj.TypeHash, obj.EncodingZipmap)
}

func (rdb *Rdber) loadListObject() *obj.Robj {
	ln := rdb.loadLen(nil)
	if ln == rdbLenErr {
		return nil
	}
	ql := list.NewQuicklist()
	for ln > 0 {
		v := rdb.genericLoadStringObject()
		zl := ds.Ziplist(v.([]byte))
		node := list.CreateQuicklistNode(&zl)
		ql.Link(node)
		ln--
	}
	return obj.New(ql, obj.TypeList, obj.EncodingQuicklist)
}

func (rdb *Rdber) loadStringObject() *obj.Robj {
	v := rdb.genericLoadStringObject()
	if v == nil {
		return nil
	}
	switch v.(type) {
	case int64:
		return obj.New(v, obj.TypeString, obj.EncodingInt)
	case []byte:
		return obj.New(sds.SDS(v.([]byte)), obj.TypeString, obj.EncodingRaw)
	default:
		return nil
	}
}

func (rdb *Rdber) genericLoadStringObject() any {
	var isEncoded bool
	ln := int(rdb.loadLen(&isEncoded))
	if isEncoded {
		v := rdb.loadStringIntObject(uint8(ln))
		if v == -1 {
			return nil
		}
		return v
	} else {
		p := make([]byte, ln, ln)
		if rdb.readRaw(p) != ln {
			return nil
		}
		return p
	}
}

// loadStringIntObject is the inverse operation of encodeInt.
func (rdb *Rdber) loadStringIntObject(typ uint8) int64 {
	var n int64
	var p []byte
	if typ == rdbEncInt8 {
		p = make([]byte, 1, 1)
		if rdb.readRaw(p) != 1 {
			return -1
		}
		n = int64(p[0])
	} else if typ == rdbEncInt16 {
		p = make([]byte, 2, 2)
		if rdb.readRaw(p) != 2 {
			return -1
		}
		n = int64(p[1]) | int64(p[2])<<8
	} else if typ == rdbEncInt32 {
		p = make([]byte, 4, 4)
		if rdb.readRaw(p) != 4 {
			return -1
		}
		v := uint32(p[0]) | uint32(p[1])<<8 | uint32(p[2])<<16 | uint32(p[3])<<24
		n = int64(v)
	}
	return n
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

func (rdb *Rdber) saveObjectType(val *obj.Robj) bool {
	switch val.Type() {
	case obj.TypeString:
		rdb.saveType(rdbTypeString)
		return saved
	case obj.TypeList:
		if val.CheckEncoding(obj.EncodingQuicklist) {
			rdb.saveType(rdbTypeList)
		}
		return saved
	case obj.TypeHash:
		if val.CheckEncoding(obj.EncodingZipmap) {
			rdb.saveType(rdbTypeHash)
		}
		return saved
	default:
		return nosave
	}
}

func (rdb *Rdber) saveObject(val *obj.Robj) bool {
	switch val.Type() {
	case obj.TypeString:
		return rdb.saveStringObject(val)
	case obj.TypeList:
		return rdb.saveListObject(val)
	case obj.TypeHash:
		return rdb.saveHashObject(val)
	default:
		return nosave
	}
}

func (rdb *Rdber) saveStringObject(val *obj.Robj) bool {
	if val.CheckEncoding(obj.EncodingInt) {
		n := val.Val().(int64)
		enc := encodeInt(n)
		if len(enc) == 0 {
			enc = util.Int64ToBytes(n)
		}
		return rdb.writeRaw(enc)
	} else if val.CheckEncoding(obj.EncodingRaw) {
		rdb.saveRawString(string(val.Val().(sds.SDS)))
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
		enc[2] = byte(n>>8) & 0xff
		enc[3] = byte(n>>16) & 0xff
		enc[4] = byte(n>>24) & 0xff
	}
	return enc
}

func (rdb *Rdber) saveListObject(val *obj.Robj) bool {
	if val.CheckEncoding(obj.EncodingQuicklist) {
		ql := val.Val().(*list.Quicklist)
		rdb.saveLen(uint64(ql.Len()))
		node := ql.Head()
		for node != nil {
			li := node.List()
			ln := li.Zlbytes()
			if !rdb.saveLen(uint64(ln)) {
				return nosave
			}
			if !rdb.writeRaw([]byte(*li)[:ln]) {
				return nosave
			}
			node = node.Next()
		}
		return saved
	}
	return nosave
}

func (rdb *Rdber) saveHashObject(val *obj.Robj) bool {
	if val.CheckEncoding(obj.EncodingZipmap) {
		zm := val.Val().(*hash.Zipmap)
		if !rdb.saveLen(uint64(zm.Len())) {
			return nosave
		}
		if !rdb.saveLen(uint64(zm.Zlbytes())) {
			return nosave
		}
		return rdb.writeRaw([]byte(*zm.Ziplist))
	}
	return nosave
}

func (rdb *Rdber) saveRawString(str string) bool {
	ln := uint64(len(str))
	if !rdb.saveLen(ln) {
		return nosave
	}
	return rdb.writeRaw([]byte(str))
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
		return rdbLenErr, isEncoded
	}

	typ := p[0] & 0xc0 >> 6
	if typ == rdbEncval {
		// In this case, the last six bits of the
		// first byte are returned which the encoding type.
		isEncoded = true
		n = uint64(p[0]) & 0x3f
	} else if typ == rdb_6bitlen {
		n = uint64(p[0]) & 0x3f
	} else if typ == rdb_14bitlen {
		p = append(p, '0')
		if rdb.readRaw(p[1:]) != 1 {
			return rdbLenErr, false
		}
		n = uint64(p[0])&0x3f<<8 | uint64(p[1])
	} else if p[0] == rdb_32bitlen {
		p = make([]byte, 4, 4)
		if rdb.readRaw(p) != 4 {
			return rdbLenErr, false
		}
		n = uint64(binary.BigEndian.Uint32(p))
	} else if p[0] == rdb_64bitlen {
		p = make([]byte, 8, 8)
		if rdb.readRaw(p) != 8 {
			return rdbLenErr, false
		}
		n = binary.BigEndian.Uint64(p)
	}
	return n, isEncoded
}

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
	}

	buf[0] = rdb_64bitlen
	if !rdb.writeRaw(buf) {
		return false
	}
	buf = make([]byte, 8, 8)
	binary.BigEndian.PutUint64(buf, ln)
	return rdb.writeRaw(buf)
}

func (rdb *Rdber) loadTime() int32 {
	var t int32
	err := binary.Read(rdb.rd, binary.LittleEndian, &t)
	return util.Cond(err != nil, -1, t)
}

func (rdb *Rdber) loadMillisecondTime() int64 {
	var t int64
	err := binary.Read(rdb.rd, binary.LittleEndian, &t)
	return util.Cond(err != nil, -1, t)
}

func (rdb *Rdber) saveMillisencondTime(t int64) bool {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, t)
	rdb.saveType(rdbOpcodeExpiretimeMs)
	rdb.writeRaw(buf.Bytes())
	return saved
}

func (rdb *Rdber) loadType() uint8 {
	p := make([]byte, 1, 1)
	return util.Cond(rdb.readRaw(p) != 1, 0, p[0])
}

func (rdb *Rdber) saveType(t uint8) bool {
	return rdb.writeRaw([]byte{t})
}

func (rdb *Rdber) readRaw(p []byte) int {
	n, _ := rdb.rd.Read(p)
	return n
}

func (rdb *Rdber) writeRaw(p []byte) bool {
	n, err := rdb.wr.Write(p)
	return err == nil && len(p) == n
}
