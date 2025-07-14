package cmd

import (
	"strconv"
	"time"

	"github.com/sunminx/RDB/internal/common"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

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

type setFlag int

func setGenericCommand(cli client, flag setFlag, key string, val, expireParam []byte) bool {
	expire := int64(-1)
	if expireParam != nil {
		var err error
		expire, err = strconv.ParseInt(string(expireParam), 10, 64)
		if err != nil || expire <= 0 {
			cli.AddReplyError([]byte("invalid expire param"))
			return true
		}
		expire *= 1e3
	}

	if expire != -1 {
		expire += time.Now().UnixMilli()
	}

	if _, exists := cli.Get(key); exists && (flag&objSetNx) != 0 {
		return false
	}

	cli.Set(time.Duration(expire), key, sds.NewRobj(val))
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
