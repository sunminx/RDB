package cmd

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/sunminx/RDB/internal/object"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

type setFlag int

const (
	objSetNoFlag setFlag = 0
	objSetNx     setFlag = 1 << iota
	objSetXx
	objSetEx
	objSetPx
)

func GetCommand(cli client) bool {
	var (
		key = cli.Key()
	)
	robj, ok := cli.Get(key)
	if !ok {
		cli.AddReplyRaw(shared.RespBulkNull)
		return OK
	}

	if robj.Type() != obj.TypeString {
		cli.AddReplyRaw(shared.RespErrWrongType)
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
			cli.AddReplyRaw(shared.RespBulkNull)
		}
	}
	return OK
}

func SetCommand(cli client) bool {
	var (
		flags     = objSetNoFlag
		expireArg = []byte{}
		unit      = unitNone
		argv      = cli.Argv()
	)
	for i := 3; i < len(argv); i++ {
		a := argv[i]
		if len(a) == 2 && (a[0] == 'n' || a[0] == 'N') &&
			(a[1] == 'x' || a[1] == 'X') && flags&objSetXx == 0 {
			flags |= objSetNx
		} else if len(a) == 2 && (a[0] == 'x' || a[0] == 'X') &&
			(a[1] == 'x' || a[1] == 'X') && flags&objSetNx == 0 {
			flags |= objSetXx
		} else if len(a) == 2 && (a[0] == 'e' || a[0] == 'E') &&
			(a[1] == 'x' || a[1] == 'X') && (i+1) < len(argv) && flags&objSetPx == 0 {
			flags |= objSetEx
			expireArg = argv[i+1]
			unit = unitSeconds
			i++
		} else if len(a) == 2 && (a[0] == 'p' || a[0] == 'P') &&
			(a[1] == 'x' || a[1] == 'X') && (i+1) < len(argv) && flags&objSetEx == 0 {
			flags |= objSetPx
			expireArg = argv[i+1]
			unit = unitMilliseconds
			i++
		}
	}
	key := cli.Key()
	expire := int64(-1)
	if len(expireArg) > 0 {
		var err error
		expire, err = strconv.ParseInt(string(expireArg), 10, 64)
		if err != nil {
			cli.AddReplyError([]byte("invalid expire time in " + key))
			return ERR
		}
	}
	return setGenericCommand(cli, flags, key, argv[2], expire, unit, true)
}

func SetNxCommand(cli client) bool {
	argv := cli.Argv()
	return setGenericCommand(cli, objSetNx, cli.Key(), argv[2], -1, unitNone, true)
}

func SetExCommand(cli client) bool {
	argv := cli.Argv()
	expire := int64(-1)
	var err error
	expire, err = strconv.ParseInt(string(argv[2]), 10, 64)
	if err != nil {
		cli.AddReplyError([]byte("invalid expire time in " + cli.Key()))
		return ERR
	}
	return setGenericCommand(cli, objSetEx, cli.Key(), argv[3], expire, unitSeconds, true)
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
				cli.AddReplyRaw(shared.RespIntZero)
				return ERR
			}
		}
	}

	for i := 1; i < size; i += 2 {
		_ = cli.Set(-1, string(argv[i]), sds.NewRobj(argv[i+1]))
	}

	cli.AddDirty((size - 1) / 2)
	if flag&objSetNx != 0 {
		cli.AddReplyRaw(shared.RespIntOne)
	} else {
		cli.AddReplyRaw(shared.RespStrOK)
	}
	return OK
}

func AppendCommand(cli client) bool {
	key := cli.Key()
	argv := cli.Argv()
	val, ok := cli.Get(key)
	if !ok {
		_ = setGenericCommand(cli, objSetEx, key, argv[2], -1, unitSeconds, false)
		goto reply
	}
	if !val.CheckType(obj.TypeString) {
		cli.AddReplyRaw(shared.RespErrWrongType)
		return ERR
	}
	sds.Append(val, argv[2])
	_ = setGenericCommand(cli, objSetEx, key, val.Val().(sds.SDS), -1, unitSeconds, false)
reply:
	ln := val.Val().(sds.SDS).Len()
	cli.AddReplyInt64(int64(ln))
	return OK
}

func setGenericCommand(cli client, flag setFlag, key string, val []byte,
	expire int64, unit int, replyInOk bool) bool {
	if expire > 0 {
		factor := int64(1e3)
		if unit == unitMilliseconds {
			factor = 1
		}
		expire *= factor
		expire += time.Now().UnixMilli()
	}
	if _, ok := cli.Get(key); ok && flag&objSetNx != 0 || !ok && flag&objSetXx != 0 {
		cli.AddReplyRaw(shared.RespBulkNull)
		return false
	}
	cli.Set(time.Duration(expire), key, sds.NewRobj(val))
	cli.AddDirty(1)
	if replyInOk {
		cli.AddReplyRaw(shared.RespStrOK)
	}
	return true
}

func StrlenCommand(cli client) bool {
	key := cli.Key()
	val, ok := cli.Get(key)
	if !ok {
		cli.AddReplyRaw(shared.RespIntZero)
		return OK
	}
	if !val.CheckType(obj.TypeString) {
		cli.AddReplyRaw(shared.RespErrWrongType)
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
		cli.AddReplyRaw(shared.RespErrWrongType)
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
		cli.AddReplyRaw(shared.RespIntOne)
	} else {
		cli.AddReplyRaw(shared.RespIntZero)
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
		cli.AddReplyRaw(shared.RespIntZero)
		return ERR
	}

	var val []byte
	switch obj.Encoding() {
	case object.EncodingRaw:
		val = obj.Val().(sds.SDS)
	case object.EncodingInt:
		val = []byte(fmt.Sprintf("%d", obj.Val().(int64)))
	default:
		cli.AddReplyRaw(shared.RespIntZero)
		return ERR

	}

	bit := getBit(val, off)

	// bit is not zero means the bit is 1 before set operation.
	if bit != 0 {
		cli.AddReplyRaw(shared.RespIntOne)
	} else {
		cli.AddReplyRaw(shared.RespIntZero)
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
			cli.AddReplyRaw(shared.RespIntZero)
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
			cli.AddReplyRaw(shared.RespErrWrongType)
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
		cli.AddReplyRaw(shared.RespBulkEmpty)
		return ERR
	}
	if !obj.CheckType(object.TypeString) {
		cli.AddReplyRaw(shared.RespErrWrongType)
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
