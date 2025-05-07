package dump

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
	"unsafe"

	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/rio"
	"github.com/sunminx/RDB/pkg/util"
)

type Rdber struct {
	rd   *rio.Reader
	wr   *rio.Writer
	info RdbInfo
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

	return nil
}

type Opcode uint8

const (
	RdbOpcodeModuleAux    Opcode = iota + 247 /* module auxiliary data. */
	RdbOpcodeIdle                = 248        /* lru idle time. */
	RdbOpcodeFreq                = 249        /* lfu frequency. */
	RdbOpcodeAux                 = 250        /* rdb aux field. */
	RdbOpcodeResizedb            = 251        /* hash table resize hint. */
	RdbOpcodeExpiretimeMs        = 252        /* expire time in milliseconds. */
	RdbOpcodeExpiretime          = 253        /* old expire time in seconds. */
	RdbOpcodeSelectdb            = 254        /* db number of the following keys. */
	RdbOpcodeEOF                 = 255
)

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

func (rdb *Rdber) saveType(op Opcode) bool {
	return rdb.writeRaw([]byte{uint8(op)})
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

func (rdb *Rdber) writeRaw(p []byte) bool {
	n, err := rdb.wr.Write(p)
	return err == nil && len(p) == n
}
