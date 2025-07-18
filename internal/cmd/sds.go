package cmd

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/object"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

type setFlag int

const (
	objSetNoFlag setFlag = 0
	objSetNx             = 1 << iota
)

func GetCommand(cli client) bool {
	robj, ok := cli.Get(cli.Key())
	if !ok {
		cli.AddReplyRaw(common.Reply["nullbulk"])
		return OK
	}

	if robj.Type() != obj.TypeString {
		cli.AddReplyError(common.Reply["wrongtypeerr"])
		return ERR
	}

	cli.AddReplyBulk(robj)
	return OK
}

func MGetCommand(cli client) bool {
	argv := cli.Argv()
	size := len(argv) - 1

	cli.AddReplyMultibulkLen(int64(size))

	for i := 1; i < len(argv); i++ {
		val, ok := cli.Get(string(argv[i]))
		if ok {
			cli.AddReplyBulk(val)
		} else {
			cli.AddReplyRaw(common.Reply["nullbulk"])
		}
	}
	return OK
}

func SetCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	for i := 2; i < len(argv); i++ {
		_ = setGenericCommand(cli, objSetNoFlag, key, argv[i], nil)
	}
	cli.AddReplyStatus(common.Reply["ok"])
	return OK
}

func SetNxCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	ok := setGenericCommand(cli, objSetNx, key, argv[2], nil)
	if !ok {
		cli.AddReplyRaw(common.Reply["czero"])
	} else {
		cli.AddReplyStatus(common.Reply["ok"])
	}
	return OK
}

func SetExCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	_ = setGenericCommand(cli, objSetNoFlag, key, argv[3], argv[2])
	cli.AddReplyStatus(common.Reply["ok"])
	return OK
}

func MSetCommand(cli client) bool {
	return msetGerenicCommand(cli, objSetNoFlag)
}

func MSetNxCommand(cli client) bool {
	return msetGerenicCommand(cli, objSetNx)
}

func msetGerenicCommand(cli client, flag setFlag) bool {
	argv := cli.Argv()
	size := len(argv)

	if size%2 == 0 {
		cli.AddReplyError([]byte("wrong number of arguments for MSET"))
		return ERR
	}

	if flag&objSetNx != 0 {
		for i := 1; i < size; i += 2 {
			if _, ok := cli.Get(string(argv[i])); ok {
				cli.AddReplyRaw(common.Reply["czero"])
				return ERR
			}
		}
	}

	for i := 1; i < size; i += 2 {
		_ = cli.Set(-1, string(argv[i]), sds.NewRobj(argv[i+1]))
	}

	cli.AddDirty((size - 1) / 2)
	if flag&objSetNx != 0 {
		cli.AddReplyRaw(common.Reply["cone"])
	} else {
		cli.AddReplyStatus(common.Reply["ok"])
	}
	return OK
}

func AppendCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	val, ok := cli.Get(key)
	if !ok {
		_ = setGenericCommand(cli, objSetNoFlag, key, argv[2], nil)
		return OK
	}
	if !val.CheckType(obj.TypeString) {
		cli.AddReplyError(common.Reply["wrongtypeerr"])
		return ERR
	}

	sds.Append(val, argv[2])
	_ = setGenericCommand(cli, objSetNoFlag, key, val.Val().(sds.SDS), nil)

	cli.AddReplyBulk(val)
	return OK
}

func setGenericCommand(cli client, flag setFlag, key string, val, expiresParam []byte) bool {
	expires := int64(-1)
	if expiresParam != nil {
		var err error
		expires, err = strconv.ParseInt(string(expiresParam), 10, 64)
		if err != nil || expires <= 0 {
			cli.AddReplyError([]byte("invalid expire param"))
			return true
		}
		expires *= 1e3
	}

	if expires != -1 {
		expires += time.Now().UnixMilli()
	}

	if _, exists := cli.Get(key); exists && (flag&objSetNx) != 0 {
		return false
	}

	cli.Set(time.Duration(expires), key, sds.NewRobj(val))
	cli.AddDirty(1)
	return true
}

func StrlenCommand(cli client) bool {
	key := cli.Key()
	val, ok := cli.Get(key)
	if !ok {
		cli.AddReplyRaw(common.Reply["czero"])
		return OK
	}
	if !val.CheckType(obj.TypeString) {
		cli.AddReplyError(common.Reply["wrongtypeerr"])
		return OK
	}
	cli.AddReplyInt64(sds.Len(val))
	return OK
}

func DelCommand(cli client) bool {
	var numdel int64
	argv := cli.Argv()
	for i := 1; i < len(argv); i++ {
		cli.Del(string(argv[i]))
		numdel += 1
	}
	cli.AddReplyInt64(numdel)
	cli.AddDirty(1)
	return OK
}

func ExistsCommand(cli client) bool {
	var numexists int64
	argv := cli.Argv()
	for i := 1; i < len(argv); i++ {
		if _, ok := cli.Get(string(argv[i])); ok {
			numexists += 1
		}
	}

	cli.AddReplyInt64(numexists)
	return OK
}

func IncrCommand(cli client) bool {
	return incrdecrCommand(cli, cli.Key(), 1)
}

func DecrCommand(cli client) bool {
	return incrdecrCommand(cli, cli.Key(), -1)
}

func incrdecrCommand(cli client, key string, n int64) bool {
	val, ok := cli.Get(key)
	if !ok || !val.CheckType(obj.TypeString) {
		cli.AddReplyError(common.Reply["wrongtypeerr"])
		return ERR
	}
	cli.AddReplyInt64(sds.Incr(val, n))
	cli.AddDirty(1)
	return OK
}

func SetBitCommand(cli client) bool {
	key := cli.Key()
	argv := cli.Argv()
	errMsg := []byte("bit is not an integer or out of range")

	var err error
	off, err := getBitOffsetFromArgument(argv[2])
	if err != nil {
		cli.AddReplyError(errMsg)
		return ERR
	}
	on, err := getBitValueFromArgument(argv[3])
	if err != nil {
		cli.AddReplyError(errMsg)
		return ERR
	}
	val, err := lookupStringForBitCommand(cli, key, off)
	if err != nil {
		cli.AddReplyError([]byte(err.Error()))
		return ERR
	}

	oldBit := getBit(val, off)
	setBit(val, off, on)
	if err := setStringForBitCommand(cli, key, val); err != nil {
		cli.AddReplyError([]byte(err.Error()))
	}

	// oldBit is not zero means the bit is 1 before set operation.
	if oldBit != 0 {
		cli.AddReplyRaw(common.Reply["cone"])
	} else {
		cli.AddReplyRaw(common.Reply["czero"])
	}
	return OK
}

func GetBitCommand(cli client) bool {
	key := cli.Key()
	argv := cli.Argv()
	errMsg := []byte("bit is not an integer or out of range")

	var err error
	off, err := getBitOffsetFromArgument(argv[2])
	if err != nil {
		cli.AddReplyError(errMsg)
		return ERR
	}
	obj, ok := cli.Get(key)
	if !ok || !obj.CheckType(object.TypeString) {
		cli.AddReplyRaw(common.Reply["czero"])
		return ERR
	}

	var val []byte
	switch obj.Encoding() {
	case object.EncodingRaw:
		val = obj.Val().(sds.SDS)
	case object.EncodingInt:
		val = []byte(fmt.Sprintf("%d", obj.Val().(int64)))
	default:
		cli.AddReplyRaw(common.Reply["czero"])
		return ERR

	}

	bit := getBit(val, off)

	// bit is not zero means the bit is 1 before set operation.
	if bit != 0 {
		cli.AddReplyRaw(common.Reply["cone"])
	} else {
		cli.AddReplyRaw(common.Reply["czero"])
	}
	return OK
}

func getBitOffsetFromArgument(val []byte) (uint64, error) {
	off, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, nil
	}

	if off < 0 || (off>>3) > (512*1024*1024) {
		return 0, errors.New("invalid bit offset")
	}

	return uint64(off), nil
}

func getBitValueFromArgument(val []byte) (uint8, error) {
	if len(val) > 1 {
		return 0, errors.New("invalid bit value")
	}
	return uint8(val[0]), nil
}

func setStringForBitCommand(cli client, key string, val []byte) error {
	obj, ok := cli.Get(key)
	if !ok {
		cli.Set(-1, key, sds.NewRobj(val))
		return nil
	}

	switch obj.Encoding() {
	case object.EncodingRaw:
		obj.SetVal(sds.New(val))
	case object.EncodingInt:
		cli.Set(-1, key, sds.NewRobj(val))
	default:
	}
	return nil
}

// lookupStringForBitCommand lookup byte slice which stores bit.
// we will set key-val while key is not exists or the capacity of byte slice is not enough.
func lookupStringForBitCommand(cli client, key string, off uint64) ([]byte, error) {
	byteLen := (off >> 3) + 1
	obj, ok := cli.Get(key)
	if !ok {
		val := make([]byte, byteLen, byteLen)
		return val, nil
	} else {
		if !obj.CheckType(object.TypeString) {
			return nil, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
		}
		switch obj.Encoding() {
		case object.EncodingRaw:
			val := obj.Val().(sds.SDS)
			if uint64(cap(val)) < byteLen {
				newVal := make([]byte, byteLen, byteLen)
				copy(newVal, val)
				return newVal, nil
			} else {
				return val, nil
			}
		case object.EncodingInt:
			val := obj.Val().(int64)
			return []byte(fmt.Sprintf("%d", val)), nil
		default:
		}
		return nil, errors.New("invalid encoding of value")
	}
}

func getBit(val []byte, off uint64) uint8 {
	bVal, ok := getByte(val, off)
	if !ok {
		return 0
	}
	bOff := getOffsetInByte(off)
	return bVal & (0x1 << bOff)
}

func setBit(val []byte, off uint64, on uint8) {
	bVal, _ := getByte(val, off)
	bOff := getOffsetInByte(off)
	bVal &= ^(1 << bOff)
	bVal |= (0x1 & on) << bOff
	idx := getByteIndex(off)
	val[idx] = bVal
}

func getByte(val []byte, off uint64) (byte, bool) {
	idx := getByteIndex(off)
	if idx >= uint64(len(val)) {
		return '\n', false
	}
	return val[idx], true
}

func getByteIndex(off uint64) uint64 {
	return off >> 3
}

func getOffsetInByte(off uint64) uint64 {
	return 7 - (off & 0x7)
}

func SetRangeCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	offset, ok := getIntFromArgvOrReply(cli, argv[2])
	if !ok {
		return ERR
	}
	if offset < 0 || offset > math.MaxInt {
		cli.AddReplyError([]byte("value is out of range"))
		return ERR
	}

	newVal := argv[3]
	newValLen := len(newVal)
	obj, ok := cli.Get(key)
	if !ok {
		if newValLen == 0 {
			cli.AddReplyRaw(common.Reply["czero"])
			return OK
		}
		if offset+newValLen > 512*1024*1024 {
			cli.AddReplyError([]byte("string exceeds maximum allowed size (512MB)"))
			return ERR
		}
		obj = sds.NewRobj(sds.New(make([]byte, offset+newValLen)))
		_ = cli.Set(-1, key, obj)
	} else {
		if !obj.CheckType(object.TypeString) {
			cli.AddReplyError(common.Reply["wrongtypeerr"])
			return ERR
		}
		if newValLen == 0 {
			cli.AddReplyInt64(sds.Len(obj))
			return OK
		}
		if offset+newValLen > 512*1024*1024 {
			cli.AddReplyError([]byte("string exceeds maximum allowed size (512MB)"))
			return ERR
		}
	}

	var val sds.SDS
	if obj.CheckEncoding(object.EncodingInt) {
		obj.SetEncoding(object.EncodingRaw)
		val = sds.New([]byte(strconv.FormatInt(obj.Val().(int64), 10)))
	} else {
		val = obj.Val().(sds.SDS)
	}
	val = val.SetAt(offset, newVal)
	obj.SetVal(val)
	cli.AddDirty(1)
	cli.AddReplyInt64(sds.Len(obj))
	return OK
}

func GetRangeCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	start, ok := getIntFromArgvOrReply(cli, argv[2])
	if !ok {
		return ERR
	}
	end, ok := getIntFromArgvOrReply(cli, argv[3])
	if !ok {
		return ERR
	}
	obj, ok := cli.Get(key)
	if !ok {
		cli.AddReplyRaw(common.Reply["emptybulk"])
		return ERR
	}
	if !obj.CheckType(object.TypeString) {
		cli.AddReplyError(common.Reply["wrongtype"])
		return ERR
	}

	var val sds.SDS
	if obj.CheckEncoding(object.EncodingRaw) {
		val = obj.Val().(sds.SDS)
	} else {
		val = sds.New([]byte(strconv.FormatInt(obj.Val().(int64), 10)))
	}
	rangeVal := val.Range(start, end)
	cli.AddReplyBulkRaw(rangeVal)
	return OK
}

func getIntFromArgvOrReply(cli client, val []byte) (int, bool) {
	n, err := strconv.Atoi(string(val))
	if err != nil {
		cli.AddReplyError([]byte(err.Error()))
		return 0, false
	}
	return n, true
}
